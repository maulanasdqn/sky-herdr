use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use regex::Regex;

use crate::api::schema::{
    AgentInfo, AgentStateCursor, AgentStatus, EmptyParams, ErrorBody, ErrorResponse, EventData,
    EventEnvelope, EventKind, EventMatch, EventsWaitParams, Method, Request, ResponseResult,
    Subscription, SubscriptionEventData, SubscriptionEventEnvelope, SuccessResponse,
};
use crate::api::server::{
    dispatch_to_app_with_caller_timeout, dispatch_to_app_with_timeout, should_stop_connection,
    APP_RESPONSE_TIMEOUT, CONNECTION_POLL_INTERVAL,
};
use crate::api::subscriptions::ActiveSubscription;
use crate::api::subscriptions::{match_output, output_match_read_source};
use crate::api::{ApiRequestSender, EventHub};
use crate::ipc::LocalStream;

const AGENT_PROMPT_EFFECT_TIMEOUT_MS: u64 = 5_000;

pub(super) fn wait_for_output(
    request_id: String,
    params: crate::api::schema::PaneWaitForOutputParams,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<String>> {
    crate::logging::api_wait_started(&request_id, &params.pane_id, params.timeout_ms);
    let deadline = params
        .timeout_ms
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));

    let regex = match &params.r#match {
        crate::api::schema::OutputMatch::Regex { value } => match Regex::new(value) {
            Ok(regex) => Some(regex),
            Err(err) => {
                return Ok(Some(
                    serde_json::to_string(&ErrorResponse {
                        id: request_id,
                        error: ErrorBody {
                            code: "invalid_regex".into(),
                            message: err.to_string(),
                        },
                    })
                    .unwrap(),
                ));
            }
        },
        crate::api::schema::OutputMatch::Substring { .. } => None,
    };

    loop {
        if should_stop_connection(stream, running)? {
            crate::logging::api_wait_completed(&request_id, &params.pane_id, "client_disconnected");
            return Ok(None);
        }

        let read_request = Request {
            id: format!("{request_id}:read"),
            method: Method::PaneRead(crate::api::schema::PaneReadParams {
                pane_id: params.pane_id.clone(),
                source: output_match_read_source(&params.source),
                lines: params.lines,
                format: crate::api::schema::ReadFormat::Text,
                strip_ansi: params.strip_ansi,
                intent: crate::api::schema::ReadIntent::Passive,
            }),
        };
        let response =
            dispatch_to_app_with_timeout(read_request, api_tx, Some(APP_RESPONSE_TIMEOUT));
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&response) else {
            return Ok(Some(response));
        };
        if value.get("error").is_some() {
            let mut value = value;
            value["id"] = serde_json::Value::String(request_id.clone());
            return Ok(Some(serde_json::to_string(&value).unwrap()));
        }

        let read_value = value["result"]["read"].clone();
        let Ok(read) = serde_json::from_value::<crate::api::schema::PaneReadResult>(read_value)
        else {
            return Ok(Some(
                serde_json::to_string(&ErrorResponse {
                    id: request_id,
                    error: ErrorBody {
                        code: "internal_error".into(),
                        message: "failed to decode pane read result".into(),
                    },
                })
                .unwrap(),
            ));
        };

        let matched_line = match_output(&read.text, &params.r#match, regex.as_ref());
        if matched_line.is_some() {
            let revision = read.revision;
            crate::logging::api_wait_completed(&request_id, &params.pane_id, "matched");
            return Ok(Some(
                serde_json::to_string(&SuccessResponse {
                    id: request_id,
                    result: ResponseResult::OutputMatched {
                        pane_id: read.pane_id.clone(),
                        revision,
                        matched_line,
                        read,
                    },
                })
                .unwrap(),
            ));
        }

        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            crate::logging::api_wait_timed_out(&request_id, &params.pane_id);
            return Ok(Some(
                serde_json::to_string(&ErrorResponse {
                    id: request_id,
                    error: ErrorBody {
                        code: "timeout".into(),
                        message: "timed out waiting for output match".into(),
                    },
                })
                .unwrap(),
            ));
        }

        std::thread::sleep(CONNECTION_POLL_INTERVAL);
    }
}

pub(super) fn wait_for_agent(
    request_id: String,
    params: crate::api::schema::AgentWaitParams,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<String>> {
    let last_event_sequence = event_hub.current_sequence();
    let initial = match agent_get(&request_id, &params.target, api_tx) {
        Ok(agent) => agent,
        Err(response) => {
            return serde_json::to_string(&response)
                .map(Some)
                .map_err(std::io::Error::other);
        }
    };
    let until = agent_wait_statuses(params.until);
    if agent_wait_matches(&initial, &until, None) {
        return agent_wait_success(request_id, initial).map(Some);
    }

    match wait_for_resolved_agent(
        request_id.clone(),
        ResolvedAgentWait {
            target: params.target,
            until,
            timeout_ms: params.timeout_ms,
            initial,
            last_event_sequence,
            after_state_change_seq: None,
            accept_transient_status: true,
            timeout_kind: AgentWaitTimeoutKind::Status,
        },
        stream,
        api_tx,
        event_hub,
        running,
    )? {
        Some(AgentWaitOutcome::Matched(agent)) => agent_wait_success(request_id, *agent).map(Some),
        Some(AgentWaitOutcome::Response(response)) => Ok(Some(response)),
        None => Ok(None),
    }
}

pub(super) fn prompt_agent(
    request_id: String,
    mut params: crate::api::schema::AgentPromptParams,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<String>> {
    let Some(wait) = params.wait.clone() else {
        return Ok(Some(dispatch_to_app_with_timeout(
            Request {
                id: request_id,
                method: Method::AgentPrompt(params),
            },
            api_tx,
            None,
        )));
    };

    let wait_started = std::time::Instant::now();
    let before_prompt = match agent_get_for_prompt(
        &request_id,
        &params.target,
        api_tx,
        wait.timeout_ms,
        wait_started,
    ) {
        Ok(agent) => agent,
        Err(response) => {
            return serde_json::to_string(&response)
                .map(Some)
                .map_err(std::io::Error::other);
        }
    };
    let prompt_started_working =
        before_prompt.agent_status == crate::api::schema::AgentStatus::Working;
    let target = params.target.clone();
    if let Some(prompt_wait) = params.wait.as_mut() {
        prompt_wait.submission_deadline = wait
            .timeout_ms
            .map(|timeout_ms| wait_started + std::time::Duration::from_millis(timeout_ms));
    }
    let last_event_sequence = event_hub.current_sequence();
    let prompt_request = Request {
        id: request_id.clone(),
        method: Method::AgentPrompt(params),
    };
    #[cfg(windows)]
    let prompt_response = dispatch_to_app_with_caller_timeout(
        prompt_request,
        api_tx,
        remaining_timeout_ms(wait.timeout_ms, wait_started).map(std::time::Duration::from_millis),
    );
    #[cfg(not(windows))]
    let prompt_response = dispatch_to_app_with_timeout(prompt_request, api_tx, None);
    let Ok(prompted) = agent_from_response(&request_id, &prompt_response) else {
        return Ok(Some(prompt_response));
    };
    if !agent_wait_identity_matches(
        &prompted,
        &before_prompt.terminal_id,
        before_prompt.name.as_deref().filter(|name| *name == target),
        before_prompt.agent.as_deref(),
    ) {
        return agent_wait_not_running(request_id).map(Some);
    }

    let prompt_activity_observed = prompt_started_working
        || matches!(
            prompted.agent_status,
            crate::api::schema::AgentStatus::Working | crate::api::schema::AgentStatus::Blocked
        );
    let prompt_state_change_seq = prompted.state_change_seq;
    let until = agent_wait_statuses(wait.until);
    let mut initial = prompted;

    if !prompt_activity_observed {
        let remaining_timeout_ms = remaining_timeout_ms(wait.timeout_ms, wait_started);
        let (effect_timeout_ms, timeout_kind) = match remaining_timeout_ms {
            Some(timeout_ms) if timeout_ms <= AGENT_PROMPT_EFFECT_TIMEOUT_MS => {
                (timeout_ms, AgentWaitTimeoutKind::Status)
            }
            _ => (
                AGENT_PROMPT_EFFECT_TIMEOUT_MS,
                AgentWaitTimeoutKind::PromptStalled {
                    timeout_ms: AGENT_PROMPT_EFFECT_TIMEOUT_MS,
                },
            ),
        };
        let Some(outcome) = wait_for_resolved_agent(
            request_id.clone(),
            ResolvedAgentWait {
                target: target.clone(),
                until: prompt_activity_statuses(),
                timeout_ms: Some(effect_timeout_ms),
                initial,
                last_event_sequence,
                after_state_change_seq: Some(prompt_state_change_seq),
                accept_transient_status: true,
                timeout_kind,
            },
            stream,
            api_tx,
            event_hub,
            running,
        )?
        else {
            return Ok(None);
        };
        initial = match outcome {
            AgentWaitOutcome::Matched(agent) => *agent,
            AgentWaitOutcome::Response(response) => return Ok(Some(response)),
        };
    }
    if agent_wait_matches(&initial, &until, None) {
        return agent_prompt_success(request_id, initial).map(Some);
    }

    let Some(outcome) = wait_for_resolved_agent(
        request_id.clone(),
        ResolvedAgentWait {
            target,
            until,
            timeout_ms: remaining_timeout_ms(wait.timeout_ms, wait_started),
            initial,
            // Replay from before submission so terminal lifecycle events consumed by
            // the activity gate still terminate this settled-state wait.
            last_event_sequence,
            after_state_change_seq: None,
            accept_transient_status: false,
            timeout_kind: AgentWaitTimeoutKind::Status,
        },
        stream,
        api_tx,
        event_hub,
        running,
    )?
    else {
        return Ok(None);
    };
    let agent = match outcome {
        AgentWaitOutcome::Matched(agent) => *agent,
        AgentWaitOutcome::Response(response) => return Ok(Some(response)),
    };
    agent_prompt_success(request_id, agent).map(Some)
}

fn remaining_timeout_ms(total_ms: Option<u64>, started: std::time::Instant) -> Option<u64> {
    total_ms.map(|total_ms| {
        let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        total_ms.saturating_sub(elapsed_ms)
    })
}

fn agent_prompt_success(
    request_id: String,
    agent: crate::api::schema::AgentInfo,
) -> std::io::Result<String> {
    serde_json::to_string(&SuccessResponse {
        id: request_id,
        result: ResponseResult::AgentPrompted { agent },
    })
    .map_err(std::io::Error::other)
}

struct ResolvedAgentWait {
    target: String,
    until: Vec<crate::api::schema::AgentStatus>,
    timeout_ms: Option<u64>,
    initial: crate::api::schema::AgentInfo,
    last_event_sequence: u64,
    after_state_change_seq: Option<u64>,
    accept_transient_status: bool,
    timeout_kind: AgentWaitTimeoutKind,
}

#[derive(Clone, Copy)]
enum AgentWaitTimeoutKind {
    Status,
    PromptStalled { timeout_ms: u64 },
}

enum AgentWaitOutcome {
    Matched(Box<crate::api::schema::AgentInfo>),
    Response(String),
}

fn wait_for_resolved_agent(
    request_id: String,
    wait: ResolvedAgentWait,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<AgentWaitOutcome>> {
    let deadline = wait
        .timeout_ms
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    let expected_terminal_id = wait.initial.terminal_id.clone();
    let expected_name = wait
        .initial
        .name
        .as_ref()
        .filter(|name| name.as_str() == wait.target)
        .cloned();
    let expected_agent = wait.initial.agent.clone();
    let pane_id = wait.initial.pane_id.clone();
    let mut last_event_sequence = wait.last_event_sequence;

    loop {
        if should_stop_connection(stream, running)? {
            return Ok(None);
        }

        let mut should_probe = false;
        let mut matched_event_status = None;
        for (sequence, event) in event_hub.events_after(last_event_sequence) {
            last_event_sequence = sequence;
            match event.data {
                EventData::PaneAgentDetected {
                    pane_id: event_pane,
                    agent,
                    released,
                    final_status,
                    ..
                } if event_pane == pane_id => {
                    if released {
                        if let Some(status) = final_status
                            .filter(|status| wait.until.contains(status))
                            .or(matched_event_status)
                        {
                            let mut matched = wait.initial.clone();
                            matched.agent_status = status;
                            return Ok(Some(AgentWaitOutcome::Matched(Box::new(matched))));
                        }
                        return agent_wait_not_running(request_id)
                            .map(AgentWaitOutcome::Response)
                            .map(Some);
                    }
                    if agent.is_some() && expected_agent.is_some() && agent != expected_agent {
                        return agent_wait_not_running(request_id)
                            .map(AgentWaitOutcome::Response)
                            .map(Some);
                    }
                    should_probe = true;
                }
                EventData::PaneAgentStatusChanged {
                    pane_id: event_pane,
                    agent_status,
                    ..
                } if event_pane == pane_id => {
                    if wait.accept_transient_status && wait.until.contains(&agent_status) {
                        matched_event_status = Some(agent_status);
                    }
                    should_probe = true;
                }
                EventData::PaneUpdated { pane } if pane.pane_id == pane_id => should_probe = true,
                EventData::PaneMoved {
                    previous_pane_id, ..
                } if previous_pane_id == pane_id => {
                    return agent_wait_not_running(request_id)
                        .map(AgentWaitOutcome::Response)
                        .map(Some);
                }
                EventData::PaneClosed {
                    pane_id: event_pane,
                    ..
                }
                | EventData::PaneExited {
                    pane_id: event_pane,
                    ..
                } if event_pane == pane_id => {
                    return agent_wait_not_running(request_id)
                        .map(AgentWaitOutcome::Response)
                        .map(Some);
                }
                _ => {}
            }
        }

        if should_probe {
            let current = match agent_get(&request_id, &wait.target, api_tx) {
                Ok(agent) => agent,
                Err(response) => {
                    return agent_wait_probe_error(response)
                        .map(AgentWaitOutcome::Response)
                        .map(Some);
                }
            };
            if !agent_wait_identity_matches(
                &current,
                &expected_terminal_id,
                expected_name.as_deref(),
                expected_agent.as_deref(),
            ) {
                return agent_wait_not_running(request_id)
                    .map(AgentWaitOutcome::Response)
                    .map(Some);
            }
            if let Some(status) = matched_event_status {
                let mut matched = current;
                matched.agent_status = status;
                return Ok(Some(AgentWaitOutcome::Matched(Box::new(matched))));
            }
            if agent_wait_matches(&current, &wait.until, wait.after_state_change_seq) {
                return Ok(Some(AgentWaitOutcome::Matched(Box::new(current))));
            }
        }

        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            let current = match agent_get(&request_id, &wait.target, api_tx) {
                Ok(agent) => agent,
                Err(response) => {
                    return agent_wait_probe_error(response)
                        .map(AgentWaitOutcome::Response)
                        .map(Some);
                }
            };
            if !agent_wait_identity_matches(
                &current,
                &expected_terminal_id,
                expected_name.as_deref(),
                expected_agent.as_deref(),
            ) {
                return agent_wait_not_running(request_id)
                    .map(AgentWaitOutcome::Response)
                    .map(Some);
            }
            if agent_wait_matches(&current, &wait.until, wait.after_state_change_seq) {
                return Ok(Some(AgentWaitOutcome::Matched(Box::new(current))));
            }
            return agent_wait_timeout(request_id, wait.timeout_kind, &current)
                .map(AgentWaitOutcome::Response)
                .map(Some);
        }
        std::thread::sleep(CONNECTION_POLL_INTERVAL);
    }
}

fn prompt_activity_statuses() -> Vec<crate::api::schema::AgentStatus> {
    vec![
        crate::api::schema::AgentStatus::Working,
        crate::api::schema::AgentStatus::Blocked,
    ]
}

fn agent_wait_statuses(
    until: Vec<crate::api::schema::AgentStatus>,
) -> Vec<crate::api::schema::AgentStatus> {
    if until.is_empty() {
        vec![
            crate::api::schema::AgentStatus::Idle,
            crate::api::schema::AgentStatus::Done,
            crate::api::schema::AgentStatus::Blocked,
        ]
    } else {
        until
    }
}

fn agent_wait_identity_matches(
    agent: &crate::api::schema::AgentInfo,
    expected_terminal_id: &str,
    expected_name: Option<&str>,
    expected_agent: Option<&str>,
) -> bool {
    agent.terminal_id == expected_terminal_id
        && expected_name.is_none_or(|name| agent.name.as_deref() == Some(name))
        && match (expected_agent, agent.agent.as_deref()) {
            (Some(expected), Some(current)) => expected == current,
            (Some(_), None) => agent.name.is_some(),
            (None, _) => true,
        }
}

fn agent_wait_matches(
    agent: &crate::api::schema::AgentInfo,
    until: &[crate::api::schema::AgentStatus],
    after_state_change_seq: Option<u64>,
) -> bool {
    until.contains(&agent.agent_status)
        && after_state_change_seq.is_none_or(|baseline| agent.state_change_seq > baseline)
}

fn agent_get(
    request_id: &str,
    target: &str,
    api_tx: &ApiRequestSender,
) -> Result<crate::api::schema::AgentInfo, ErrorResponse> {
    let response = dispatch_to_app_with_timeout(
        Request {
            id: format!("{request_id}:agent"),
            method: Method::AgentGet(crate::api::schema::AgentTarget {
                target: target.to_string(),
            }),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    agent_from_response(request_id, &response)
}

fn agent_get_for_prompt(
    request_id: &str,
    target: &str,
    api_tx: &ApiRequestSender,
    total_timeout_ms: Option<u64>,
    started: std::time::Instant,
) -> Result<crate::api::schema::AgentInfo, ErrorResponse> {
    let request = Request {
        id: format!("{request_id}:agent"),
        method: Method::AgentGet(crate::api::schema::AgentTarget {
            target: target.to_string(),
        }),
    };
    let remaining_ms = remaining_timeout_ms(total_timeout_ms, started);
    let response = match remaining_ms {
        Some(timeout_ms) if timeout_ms <= APP_RESPONSE_TIMEOUT.as_millis() as u64 => {
            dispatch_to_app_with_caller_timeout(
                request,
                api_tx,
                Some(std::time::Duration::from_millis(timeout_ms)),
            )
        }
        _ => dispatch_to_app_with_timeout(request, api_tx, Some(APP_RESPONSE_TIMEOUT)),
    };
    agent_from_response(request_id, &response)
}

fn agent_from_response(
    request_id: &str,
    response: &str,
) -> Result<crate::api::schema::AgentInfo, ErrorResponse> {
    let value: serde_json::Value = serde_json::from_str(response).map_err(|_| ErrorResponse {
        id: request_id.into(),
        error: ErrorBody {
            code: "internal_error".into(),
            message: "failed to decode agent response".into(),
        },
    })?;
    if value.get("error").is_some() {
        let error = serde_json::from_value(value["error"].clone()).map_err(|_| ErrorResponse {
            id: request_id.into(),
            error: ErrorBody {
                code: "internal_error".into(),
                message: "failed to decode agent error".into(),
            },
        })?;
        return Err(ErrorResponse {
            id: request_id.into(),
            error,
        });
    }
    serde_json::from_value(value["result"]["agent"].clone()).map_err(|_| ErrorResponse {
        id: request_id.into(),
        error: ErrorBody {
            code: "internal_error".into(),
            message: "failed to decode agent result".into(),
        },
    })
}

fn agent_wait_success(
    request_id: String,
    agent: crate::api::schema::AgentInfo,
) -> std::io::Result<String> {
    serde_json::to_string(&SuccessResponse {
        id: request_id,
        result: ResponseResult::AgentInfo { agent },
    })
    .map_err(std::io::Error::other)
}

fn agent_wait_timeout(
    request_id: String,
    kind: AgentWaitTimeoutKind,
    current: &crate::api::schema::AgentInfo,
) -> std::io::Result<String> {
    let (code, message) = match kind {
        AgentWaitTimeoutKind::Status => {
            ("timeout", "timed out waiting for agent status".to_string())
        }
        AgentWaitTimeoutKind::PromptStalled { timeout_ms } => {
            let status = format!("{:?}", current.agent_status).to_ascii_lowercase();
            (
                "agent_prompt_stalled",
                format!(
                    "agent prompt produced no observed working or blocked state within {timeout_ms} ms; current status is {status}"
                ),
            )
        }
    };
    serde_json::to_string(&ErrorResponse {
        id: request_id,
        error: ErrorBody {
            code: code.into(),
            message,
        },
    })
    .map_err(std::io::Error::other)
}

fn agent_wait_not_running(request_id: String) -> std::io::Result<String> {
    serde_json::to_string(&ErrorResponse {
        id: request_id,
        error: ErrorBody {
            code: "agent_not_running".into(),
            message: "agent is no longer running in the target pane".into(),
        },
    })
    .map_err(std::io::Error::other)
}

fn agent_wait_probe_error(response: ErrorResponse) -> std::io::Result<String> {
    if response.error.code == "agent_not_found" {
        return agent_wait_not_running(response.id);
    }
    serde_json::to_string(&response).map_err(std::io::Error::other)
}

pub(super) fn wait_for_agents(
    request_id: String,
    params: crate::api::schema::AgentWaitManyParams,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<String>> {
    if params.targets.is_empty() {
        return wait_error(
            request_id,
            "invalid_agent_targets",
            "agent.wait_many requires at least one target".into(),
        )
        .map(Some);
    }
    let since = match params.since.as_deref() {
        None => None,
        Some(value) => match AgentStateCursor::parse(value) {
            Some(cursor) => Some(cursor),
            None => {
                return wait_error(
                    request_id,
                    "invalid_agent_state_cursor",
                    format!("invalid agent state cursor {value}"),
                )
                .map(Some);
            }
        },
    };
    let deadline = params
        .timeout_ms
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    let mut last_event_sequence = event_hub.current_sequence();
    let mut group = AgentWaitGroup::new(agent_wait_statuses(params.until), params.all);
    for target in params.targets {
        match agent_get(&request_id, &target, api_tx) {
            Ok(agent) => group.track(&target, agent),
            Err(response) if response.error.code == "agent_not_found" => group.miss(target),
            Err(response) => {
                return serde_json::to_string(&response)
                    .map(Some)
                    .map_err(std::io::Error::other);
            }
        }
    }

    // The first probe also learns the server cursor that decides whether `since` applies.
    let mut should_probe = true;
    loop {
        if should_stop_connection(stream, running)? {
            return Ok(None);
        }

        match event_hub.events_after_checked(last_event_sequence) {
            Ok(events) => {
                for (sequence, event) in events {
                    last_event_sequence = sequence;
                    should_probe |= group.observe_event(&event.data);
                }
            }
            // Evicted history may hide a change, so fall back to the authoritative list.
            Err(_) => {
                last_event_sequence = event_hub.current_sequence();
                should_probe = true;
            }
        }

        let timed_out = deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline);
        if should_probe || timed_out {
            should_probe = false;
            let (agents, cursor) = match agent_list(&request_id, api_tx) {
                Ok(listed) => listed,
                Err(response) => {
                    return serde_json::to_string(&response)
                        .map(Some)
                        .map_err(std::io::Error::other);
                }
            };
            let since_seq = since
                .filter(|since| since.epoch == cursor.epoch)
                .map(|since| since.seq);
            group.observe_agents(&agents, since_seq);
            if group.is_finished() {
                return serde_json::to_string(&SuccessResponse {
                    id: request_id,
                    result: group.into_result(cursor),
                })
                .map(Some)
                .map_err(std::io::Error::other);
            }
        }
        if timed_out {
            return wait_error(
                request_id,
                "timeout",
                "timed out waiting for agent status".into(),
            )
            .map(Some);
        }
        std::thread::sleep(CONNECTION_POLL_INTERVAL);
    }
}

/// Settle rules for one `agent.wait_many` request, kept free of I/O.
///
/// Members are pinned by terminal identity like `agent.wait`, so a replacement
/// occupant or a pane move cannot satisfy the wait under a stale target.
struct AgentWaitGroup {
    until: Vec<AgentStatus>,
    all: bool,
    members: Vec<AgentWaitMember>,
    missing: Vec<String>,
}

struct AgentWaitMember {
    expected_name: Option<String>,
    expected_agent: Option<String>,
    agent: AgentInfo,
    /// Requested state seen in an event, kept in case it passed before the next probe.
    observed_status: Option<AgentStatus>,
    outcome: Option<AgentWaitMemberOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentWaitMemberOutcome {
    Settled,
    Exited,
}

impl AgentWaitGroup {
    fn new(until: Vec<AgentStatus>, all: bool) -> Self {
        Self {
            until,
            all,
            members: Vec::new(),
            missing: Vec::new(),
        }
    }

    fn track(&mut self, target: &str, agent: AgentInfo) {
        if self
            .members
            .iter()
            .any(|member| member.agent.terminal_id == agent.terminal_id)
        {
            return;
        }
        self.members.push(AgentWaitMember {
            expected_name: agent.name.clone().filter(|name| name == target),
            expected_agent: agent.agent.clone(),
            agent,
            observed_status: None,
            outcome: None,
        });
    }

    fn miss(&mut self, target: String) {
        if !self.missing.contains(&target) {
            self.missing.push(target);
        }
    }

    /// Records event evidence and reports whether a pending member may have changed.
    fn observe_event(&mut self, event: &EventData) -> bool {
        let (pane_id, observed_status, released) = match event {
            EventData::PaneAgentStatusChanged {
                pane_id,
                agent_status,
                ..
            } => (pane_id, Some(*agent_status), false),
            EventData::PaneAgentDetected {
                pane_id,
                released,
                final_status,
                ..
            } => (pane_id, *final_status, *released),
            EventData::PaneUpdated { pane } => (&pane.pane_id, None, false),
            EventData::PaneMoved {
                previous_pane_id, ..
            } => (previous_pane_id, None, false),
            EventData::PaneClosed { pane_id, .. } | EventData::PaneExited { pane_id, .. } => {
                (pane_id, None, false)
            }
            _ => return false,
        };
        let until = &self.until;
        let mut affected = false;
        for member in self
            .members
            .iter_mut()
            .filter(|member| member.outcome.is_none() && member.agent.pane_id == *pane_id)
        {
            affected = true;
            // A release is authoritative even while the pane briefly keeps the agent name.
            if released {
                if let Some(status) = observed_status {
                    member.agent.agent_status = status;
                }
                member.outcome = Some(AgentWaitMemberOutcome::Exited);
            } else if let Some(status) = observed_status.filter(|status| until.contains(status)) {
                member.observed_status = Some(status);
            }
        }
        affected
    }

    /// Classifies pending members against one `agent.list` snapshot. A member
    /// settles when it is in a requested state reached after `since`, or when an
    /// event showed it passing through one since the previous probe.
    fn observe_agents(&mut self, agents: &[AgentInfo], since: Option<u64>) {
        let until = &self.until;
        for member in self
            .members
            .iter_mut()
            .filter(|member| member.outcome.is_none())
        {
            let current = agents.iter().find(|agent| {
                agent_wait_identity_matches(
                    agent,
                    &member.agent.terminal_id,
                    member.expected_name.as_deref(),
                    member.expected_agent.as_deref(),
                )
            });
            let Some(current) = current else {
                if let Some(status) = member.observed_status {
                    member.agent.agent_status = status;
                }
                member.outcome = Some(AgentWaitMemberOutcome::Exited);
                continue;
            };
            member.agent = current.clone();
            if agent_wait_matches(current, until, since) {
                member.outcome = Some(AgentWaitMemberOutcome::Settled);
            } else if let Some(status) = member.observed_status {
                member.agent.agent_status = status;
                member.outcome = Some(AgentWaitMemberOutcome::Settled);
            }
        }
    }

    fn is_finished(&self) -> bool {
        if self.all {
            self.members.iter().all(|member| member.outcome.is_some())
        } else {
            !self.missing.is_empty() || self.members.iter().any(|member| member.outcome.is_some())
        }
    }

    fn into_result(self, cursor: AgentStateCursor) -> ResponseResult {
        let mut settled = Vec::new();
        let mut pending = Vec::new();
        let mut exited = Vec::new();
        for member in self.members {
            match member.outcome {
                Some(AgentWaitMemberOutcome::Settled) => settled.push(member.agent),
                Some(AgentWaitMemberOutcome::Exited) => exited.push(member.agent),
                None => pending.push(member.agent),
            }
        }
        ResponseResult::AgentWaitMany {
            settled,
            pending,
            exited,
            missing: self.missing,
            state_cursor: cursor.encode(),
        }
    }
}

fn agent_list(
    request_id: &str,
    api_tx: &ApiRequestSender,
) -> Result<(Vec<AgentInfo>, AgentStateCursor), ErrorResponse> {
    let response = dispatch_to_app_with_timeout(
        Request {
            id: format!("{request_id}:agents"),
            method: Method::AgentList(EmptyParams::default()),
        },
        api_tx,
        Some(APP_RESPONSE_TIMEOUT),
    );
    let internal_error = |message: &str| ErrorResponse {
        id: request_id.into(),
        error: ErrorBody {
            code: "internal_error".into(),
            message: message.into(),
        },
    };
    let value: serde_json::Value = serde_json::from_str(&response)
        .map_err(|_| internal_error("failed to decode agent list response"))?;
    if value.get("error").is_some() {
        let error = serde_json::from_value(value["error"].clone())
            .map_err(|_| internal_error("failed to decode agent list error"))?;
        return Err(ErrorResponse {
            id: request_id.into(),
            error,
        });
    }
    let agents = serde_json::from_value(value["result"]["agents"].clone())
        .map_err(|_| internal_error("failed to decode agent list result"))?;
    let cursor = value["result"]["state_cursor"]
        .as_str()
        .and_then(AgentStateCursor::parse)
        .ok_or_else(|| internal_error("agent list result did not include a state cursor"))?;
    Ok((agents, cursor))
}

fn wait_error(request_id: String, code: &str, message: String) -> std::io::Result<String> {
    serde_json::to_string(&ErrorResponse {
        id: request_id,
        error: ErrorBody {
            code: code.into(),
            message,
        },
    })
    .map_err(std::io::Error::other)
}

pub(super) fn wait_for_event(
    request_id: String,
    params: EventsWaitParams,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<String>> {
    let deadline = params
        .timeout_ms
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));

    let subscription = match event_match_subscription(&request_id, params.match_event) {
        Ok(subscription) => subscription,
        Err(response) => return Ok(Some(serde_json::to_string(&response).unwrap())),
    };
    let mut active = match ActiveSubscription::new(
        subscription,
        &request_id,
        0,
        api_tx,
        event_hub,
        event_hub.current_sequence(),
    ) {
        Ok(active) => active,
        Err(response) => return Ok(Some(serde_json::to_string(&response).unwrap())),
    };

    loop {
        if should_stop_connection(stream, running)? {
            return Ok(None);
        }

        match active.poll_for_wait(api_tx, event_hub) {
            Ok(Some(event)) => return Ok(Some(wait_matched_response(&request_id, event))),
            Ok(None) => {}
            Err(mut response) if response.error.code == "pane_not_found" => {
                response.id = request_id;
                return serde_json::to_string(&response)
                    .map(Some)
                    .map_err(std::io::Error::other);
            }
            Err(_) => {}
        }

        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Ok(Some(
                serde_json::to_string(&ErrorResponse {
                    id: request_id,
                    error: ErrorBody {
                        code: "timeout".into(),
                        message: "timed out waiting for event match".into(),
                    },
                })
                .unwrap(),
            ));
        }

        std::thread::sleep(CONNECTION_POLL_INTERVAL);
    }
}

fn event_match_subscription(
    request_id: &str,
    match_event: EventMatch,
) -> Result<Subscription, ErrorResponse> {
    match match_event {
        EventMatch::PaneAgentStatusChanged {
            pane_id,
            agent_status,
        } => Ok(Subscription::PaneAgentStatusChanged {
            pane_id,
            agent_status: Some(agent_status),
        }),
        _ => Err(ErrorResponse {
            id: request_id.into(),
            error: ErrorBody {
                code: "unsupported_event_wait_match".into(),
                message: "events.wait currently supports pane agent status matches".into(),
            },
        }),
    }
}

fn wait_matched_response(request_id: &str, event: serde_json::Value) -> String {
    let Ok(event) = serde_json::from_value::<SubscriptionEventEnvelope>(event) else {
        return serde_json::to_string(&ErrorResponse {
            id: request_id.into(),
            error: ErrorBody {
                code: "internal_error".into(),
                message: "failed to decode matched event".into(),
            },
        })
        .unwrap();
    };

    let SubscriptionEventData::PaneAgentStatusChanged(data) = event.data else {
        return serde_json::to_string(&ErrorResponse {
            id: request_id.into(),
            error: ErrorBody {
                code: "unsupported_event_wait_match".into(),
                message: "events.wait currently supports pane agent status matches".into(),
            },
        })
        .unwrap();
    };

    serde_json::to_string(&SuccessResponse {
        id: request_id.into(),
        result: ResponseResult::WaitMatched {
            event: EventEnvelope {
                event: EventKind::PaneAgentStatusChanged,
                data: EventData::PaneAgentStatusChanged {
                    pane_id: data.pane_id,
                    workspace_id: data.workspace_id,
                    agent_status: data.agent_status,
                    agent: data.agent,
                    title: data.title,
                    display_agent: data.display_agent,
                    state_labels: data.state_labels,
                },
            },
        },
    })
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(terminal_id: &str, name: &str, status: AgentStatus, seq: u64) -> AgentInfo {
        AgentInfo {
            terminal_id: terminal_id.into(),
            name: Some(name.into()),
            agent: Some("pi".into()),
            title: None,
            terminal_title: None,
            terminal_title_stripped: None,
            display_agent: None,
            agent_status: status,
            screen_detection_skipped: false,
            state_labels: Default::default(),
            tokens: Default::default(),
            agent_session: None,
            spawned_by: None,
            workspace_id: "w1".into(),
            tab_id: "w1:t1".into(),
            pane_id: format!("w1:{terminal_id}"),
            focused: false,
            launch_pending: false,
            interactive_ready: true,
            state_change_seq: seq,
            completion_seq: None,
            cwd: None,
            foreground_cwd: None,
            revision: 0,
        }
    }

    fn status_event(pane_id: &str, agent_status: AgentStatus) -> EventData {
        EventData::PaneAgentStatusChanged {
            pane_id: pane_id.into(),
            workspace_id: "w1".into(),
            agent_status,
            agent: Some("pi".into()),
            title: None,
            display_agent: None,
            state_labels: Default::default(),
        }
    }

    fn names(agents: &[AgentInfo]) -> Vec<&str> {
        agents
            .iter()
            .map(|agent| agent.name.as_deref().unwrap_or_default())
            .collect()
    }

    fn group(all: bool, members: &[AgentInfo]) -> AgentWaitGroup {
        let mut group = AgentWaitGroup::new(agent_wait_statuses(Vec::new()), all);
        for member in members {
            group.track(member.name.as_deref().unwrap(), member.clone());
        }
        group
    }

    const CURSOR: AgentStateCursor = AgentStateCursor { epoch: 7, seq: 40 };

    fn wait_many_result(
        group: AgentWaitGroup,
    ) -> (Vec<AgentInfo>, Vec<AgentInfo>, Vec<AgentInfo>, Vec<String>) {
        let ResponseResult::AgentWaitMany {
            settled,
            pending,
            exited,
            missing,
            state_cursor,
        } = group.into_result(CURSOR)
        else {
            panic!("expected agent_wait_many result");
        };
        assert_eq!(state_cursor, CURSOR.encode());
        (settled, pending, exited, missing)
    }

    #[test]
    fn agent_state_cursors_round_trip_and_reject_malformed_values() {
        assert_eq!(AgentStateCursor::parse(&CURSOR.encode()), Some(CURSOR));
        for value in ["", "7", "7.", ".40", "zz.40", "7.-1", "7.40.1"] {
            assert_eq!(AgentStateCursor::parse(value), None, "{value:?}");
        }
    }

    #[test]
    fn wait_many_any_returns_every_member_settled_in_the_same_snapshot() {
        let a = agent("a", "a", AgentStatus::Working, 1);
        let b = agent("b", "b", AgentStatus::Working, 2);
        let c = agent("c", "c", AgentStatus::Working, 3);
        let mut group = group(false, &[a.clone(), b.clone(), c.clone()]);

        group.observe_agents(&[a.clone(), b.clone(), c.clone()], None);
        assert!(!group.is_finished());

        let b_done = agent("b", "b", AgentStatus::Done, 8);
        let c_blocked = agent("c", "c", AgentStatus::Blocked, 9);
        group.observe_agents(&[a, b_done, c_blocked], None);
        assert!(group.is_finished());
        let (settled, pending, exited, missing) = wait_many_result(group);
        assert_eq!(names(&settled), ["b", "c"]);
        assert_eq!(names(&pending), ["a"]);
        assert!(exited.is_empty() && missing.is_empty());
    }

    #[test]
    fn wait_many_since_ignores_states_reached_before_the_cursor() {
        let idle_before = agent("a", "a", AgentStatus::Idle, 5);
        let mut group = group(false, std::slice::from_ref(&idle_before));

        group.observe_agents(std::slice::from_ref(&idle_before), Some(5));
        assert!(
            !group.is_finished(),
            "a state at the cursor already counted"
        );

        group.observe_agents(&[agent("a", "a", AgentStatus::Working, 6)], Some(5));
        assert!(!group.is_finished());
        group.observe_agents(&[agent("a", "a", AgentStatus::Idle, 7)], Some(5));
        assert!(group.is_finished());
    }

    #[test]
    fn wait_many_counts_a_requested_state_that_passed_between_probes() {
        let working = agent("a", "a", AgentStatus::Working, 1);
        let mut group = group(false, std::slice::from_ref(&working));
        group.observe_agents(std::slice::from_ref(&working), None);

        assert!(group.observe_event(&status_event("w1:a", AgentStatus::Blocked)));
        group.observe_agents(&[agent("a", "a", AgentStatus::Working, 3)], None);

        let (settled, ..) = wait_many_result(group);
        assert_eq!(settled[0].agent_status, AgentStatus::Blocked);
    }

    #[test]
    fn wait_many_reports_a_replaced_or_vanished_member_as_exited() {
        let a = agent("a", "a", AgentStatus::Working, 1);
        let b = agent("b", "b", AgentStatus::Working, 2);
        let mut group = group(true, &[a, b]);

        let mut replaced = agent("a", "a", AgentStatus::Idle, 4);
        replaced.agent = Some("codex".into());
        group.observe_agents(&[replaced], None);

        assert!(group.is_finished());
        let (settled, pending, exited, _) = wait_many_result(group);
        assert!(settled.is_empty() && pending.is_empty());
        assert_eq!(names(&exited), ["a", "b"]);
        assert_eq!(exited[0].agent.as_deref(), Some("pi"));
    }

    #[test]
    fn wait_many_reports_a_released_member_as_exited_while_its_name_lingers() {
        let idle = agent("a", "a", AgentStatus::Idle, 5);
        let mut group = group(false, std::slice::from_ref(&idle));
        group.observe_agents(std::slice::from_ref(&idle), Some(5));

        assert!(group.observe_event(&EventData::PaneAgentDetected {
            pane_id: "w1:a".into(),
            workspace_id: "w1".into(),
            agent: None,
            released: true,
            final_status: Some(AgentStatus::Idle),
        }));
        let mut lingering = idle.clone();
        lingering.agent = None;
        group.observe_agents(&[lingering], Some(5));

        let (settled, _, exited, _) = wait_many_result(group);
        assert!(settled.is_empty());
        assert_eq!(names(&exited), ["a"]);
        assert_eq!(exited[0].agent_status, AgentStatus::Idle);
    }

    #[test]
    fn wait_many_all_waits_for_every_member_and_keeps_the_settled_snapshot() {
        let a = agent("a", "a", AgentStatus::Working, 1);
        let b = agent("b", "b", AgentStatus::Working, 2);
        let mut group = group(true, &[a, b.clone()]);

        group.observe_agents(&[agent("a", "a", AgentStatus::Idle, 3), b.clone()], None);
        assert!(!group.is_finished());

        group.observe_agents(
            &[
                agent("a", "a", AgentStatus::Working, 4),
                agent("b", "b", AgentStatus::Idle, 5),
            ],
            None,
        );
        assert!(group.is_finished());
        let (settled, ..) = wait_many_result(group);
        assert_eq!(names(&settled), ["a", "b"]);
        assert_eq!(settled[0].agent_status, AgentStatus::Idle);
    }

    #[test]
    fn wait_many_missing_targets_end_an_any_wait_but_not_an_all_wait() {
        let working = agent("a", "a", AgentStatus::Working, 1);

        let mut any = group(false, std::slice::from_ref(&working));
        any.miss("ghost".into());
        any.observe_agents(std::slice::from_ref(&working), None);
        assert!(any.is_finished());

        let mut all = group(true, std::slice::from_ref(&working));
        all.miss("ghost".into());
        all.observe_agents(std::slice::from_ref(&working), None);
        assert!(!all.is_finished());
    }

    #[test]
    fn wait_many_tracks_each_terminal_once_and_ignores_unrelated_events() {
        let a = agent("a", "a", AgentStatus::Working, 1);
        let mut group = AgentWaitGroup::new(agent_wait_statuses(Vec::new()), false);
        group.track("a", a.clone());
        group.track("w1:a", a);
        assert_eq!(group.members.len(), 1);

        assert!(!group.observe_event(&status_event("w1:other", AgentStatus::Idle)));
        assert!(group.members[0].observed_status.is_none());
    }

    #[test]
    fn agent_wait_probe_only_translates_agent_disappearance() {
        let disappeared = agent_wait_probe_error(ErrorResponse {
            id: "wait".into(),
            error: ErrorBody {
                code: "agent_not_found".into(),
                message: "missing".into(),
            },
        })
        .unwrap();
        let disappeared: ErrorResponse = serde_json::from_str(&disappeared).unwrap();
        assert_eq!(disappeared.id, "wait");
        assert_eq!(disappeared.error.code, "agent_not_running");

        let unavailable = agent_wait_probe_error(ErrorResponse {
            id: "wait".into(),
            error: ErrorBody {
                code: "server_unavailable".into(),
                message: "timed out waiting for app response".into(),
            },
        })
        .unwrap();
        let unavailable: ErrorResponse = serde_json::from_str(&unavailable).unwrap();
        assert_eq!(unavailable.id, "wait");
        assert_eq!(unavailable.error.code, "server_unavailable");
    }
}
