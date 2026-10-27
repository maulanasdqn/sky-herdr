use super::*;
use crate::protocol::ClientShellAgentSpawner;

fn pane(pane_id: &str, label: Option<&str>) -> ClientShellPane {
    ClientShellPane {
        pane_id: pane_id.into(),
        workspace_id: "ws_1".into(),
        tab_id: "tab_1".into(),
        label: label.map(str::to_owned),
        cwd: Some("/repo".into()),
        foreground_cwd: Some("/repo".into()),
        focused: false,
        right_click_passthrough: false,
    }
}

fn agent(
    pane_id: &str,
    name: Option<&str>,
    status: AgentStatus,
    spawned_by: Option<(&str, Option<&str>)>,
) -> ClientShellAgent {
    ClientShellAgent {
        pane_id: pane_id.into(),
        workspace_id: "ws_1".into(),
        tab_id: "tab_1".into(),
        name: name.map(str::to_owned),
        display_agent: None,
        agent: Some("claude".into()),
        title: None,
        terminal_title: None,
        terminal_title_stripped: None,
        agent_status: status,
        state_change_seq: 1,
        state_labels: Vec::new(),
        tokens: Vec::new(),
        focused: false,
        spawned_by: spawned_by.map(|(terminal_id, pane_id)| ClientShellAgentSpawner {
            terminal_id: terminal_id.into(),
            pane_id: pane_id.map(str::to_owned),
            name: None,
        }),
    }
}

/// `pane_1` hosts an unnamed lead agent; `pane_5` is a plain shell running a script.
fn teams_snapshot() -> ClientShellSnapshot {
    let mut snapshot = snapshot();
    snapshot.panes.extend([
        pane("pane_2", None),
        pane("pane_3", None),
        pane("pane_4", None),
        pane("pane_5", Some("script")),
        pane("pane_6", None),
    ]);
    snapshot.agents = vec![
        agent("pane_1", None, AgentStatus::Working, None),
        agent(
            "pane_6",
            Some("docs"),
            AgentStatus::Blocked,
            Some(("term_gone", None)),
        ),
        agent(
            "pane_4",
            Some("tests"),
            AgentStatus::Idle,
            Some(("term_script", Some("pane_5"))),
        ),
        agent(
            "pane_2",
            Some("api"),
            AgentStatus::Working,
            Some(("term_lead", Some("pane_1"))),
        ),
        agent(
            "pane_3",
            Some("ui"),
            AgentStatus::Idle,
            Some(("term_lead", Some("pane_1"))),
        ),
    ];
    snapshot
}

fn teams_state(snapshot: ClientShellSnapshot) -> ClientShellState {
    let mut state = ClientShellState::new(ClientShellConfig::from_config(&Config::default()));
    state.set_snapshot(Box::new(snapshot));
    state.set_pane_surface(surface());
    state
}

fn click(state: &mut ClientShellState, rect: Rect) -> ClientShellInput {
    state.handle_raw_events(vec![RawInputEvent::Mouse(MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Left),
        column: rect.x,
        row: rect.y,
        modifiers: KeyModifiers::empty(),
    })])
}

fn focused_pane(input: &ClientShellInput) -> Option<&str> {
    match &input.actions[..] {
        [ClientShellAction::Endpoint { request, .. }] => match &request.method {
            crate::api::schema::Method::PaneFocus(target) => Some(&target.pane_id),
            _ => None,
        },
        _ => None,
    }
}

#[test]
fn agent_teams_group_workers_by_spawner_in_spawner_pane_order() {
    let snapshot = teams_snapshot();
    let teams = super::super::agent_teams::agent_teams(&snapshot);
    let summary: Vec<(&str, Vec<&str>)> = teams
        .iter()
        .map(|team| {
            (
                team.spawner.terminal_id.as_str(),
                team.workers
                    .iter()
                    .map(|worker| worker.name.as_deref().unwrap_or_default())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("term_lead", vec!["api", "ui"]),
            ("term_script", vec!["tests"]),
            ("term_gone", vec!["docs"]),
        ]
    );
}

#[test]
fn agent_teams_panel_renders_on_the_right_and_focuses_rows() {
    let mut state = teams_state(teams_snapshot());
    let layout = state.layout(120, 30);
    assert_eq!(layout.agent_teams, Rect::new(90, 0, 30, 30));
    assert_eq!(layout.pane_surface.right(), 90);
    assert_eq!(layout.tab_bar.right(), 90);

    let frame = state.compose(120, 30).expect("agent teams frame");
    let panel: Vec<String> = frame_rows(&frame)
        .into_iter()
        .map(|row| row.chars().skip(90).collect::<String>())
        .collect();
    let rows: Vec<&str> = panel.iter().map(|row| row.trim_end()).collect();
    assert_eq!(rows[0], "│ agent teams               ×");
    assert_eq!(
        &rows[2..11],
        [
            "│ ● claude",
            "│  ├ ● api            working",
            "│  └ ○ ui                idle",
            "│",
            "│ ▸ script",
            "│  └ ○ tests             idle",
            "│",
            "│ ▸ closed pane",
            "│  └ ● docs           blocked",
        ],
        "panel:\n{}",
        panel.join("\n")
    );

    let panel_row = |state: &ClientShellState, target: &str| {
        state
            .hits
            .agents
            .iter()
            .find(|(rect, pane_id)| pane_id == target && rect.x >= layout.agent_teams.x)
            .map(|(rect, _)| *rect)
    };
    let api_row = panel_row(&state, "pane_2").expect("worker row hit");
    assert_eq!(api_row.x, 91);
    assert_eq!(focused_pane(&click(&mut state, api_row)), Some("pane_2"));

    state.compose(120, 30).expect("agent teams frame");
    let script_row = panel_row(&state, "pane_5").expect("shell spawner row hit");
    assert_eq!(focused_pane(&click(&mut state, script_row)), Some("pane_5"));
    assert!(
        !state
            .hits
            .agents
            .iter()
            .any(|(rect, _)| rect.y == 9 && rect.x >= 90),
        "a closed spawner has no focus target"
    );

    let short = state.compose(120, 8).expect("short agent teams frame");
    let last = frame_rows(&short)[7].chars().skip(90).collect::<String>();
    assert_eq!(last.trim_end(), "│ +2 more");
}

#[test]
fn agent_teams_panel_takes_space_only_while_agents_have_spawners() {
    let mut plain = snapshot();
    plain.agents = vec![agent("pane_1", Some("solo"), AgentStatus::Idle, None)];
    let mut state = teams_state(plain);
    let without = state.surface_size(120, 30);
    assert_eq!(state.layout(120, 30).agent_teams.width, 0);

    let mut toggled = ClientShellInput::default();
    state.record_binding(
        crate::input::KeybindMatch::Action(crate::input::KeybindAction::ToggleAgentTeams),
        &mut toggled,
    );
    assert!(state.agent_teams_hidden);
    assert!(
        !toggled.resize,
        "hiding an absent panel keeps the pane surface"
    );
    assert!(state.pane_surface.is_some());
    state.record_binding(
        crate::input::KeybindMatch::Action(crate::input::KeybindAction::ToggleAgentTeams),
        &mut ClientShellInput::default(),
    );

    state.set_snapshot(Box::new(teams_snapshot()));
    let with = state.surface_size(120, 30);
    assert_eq!(with.cols + 30, without.cols);
    assert_eq!(with.rows, without.rows);
    let sidebar = state.layout(120, 30).sidebar.width;
    let narrow = sidebar + 30 + 39;
    assert_eq!(
        state.layout(narrow, 30).agent_teams.width,
        0,
        "narrow terminals keep the panes"
    );
    state.set_pane_surface(surface());
    state.compose(narrow, 30).expect("narrow frame");
    let mut narrow_toggle = ClientShellInput::default();
    state.record_binding(
        crate::input::KeybindMatch::Action(crate::input::KeybindAction::ToggleAgentTeams),
        &mut narrow_toggle,
    );
    assert!(
        !narrow_toggle.resize && state.pane_surface.is_some(),
        "a panel that does not fit changes no geometry"
    );
    state.agent_teams_hidden = false;

    state.compose(120, 30).expect("agent teams frame");
    let mut hidden = ClientShellInput::default();
    state.record_binding(
        crate::input::KeybindMatch::Action(crate::input::KeybindAction::ToggleAgentTeams),
        &mut hidden,
    );
    assert!(hidden.resize && hidden.repaint);
    assert_eq!(state.surface_size(120, 30), without);

    state.agent_teams_hidden = false;
    state.set_pane_surface(surface());
    state.compose(120, 30).expect("agent teams frame");
    let close = state.hits.agent_teams_close;
    assert!(!close.is_empty());
    assert!(click(&mut state, close).resize);
    assert!(state.agent_teams_hidden);
}
