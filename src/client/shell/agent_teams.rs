//! Right-hand panel that lists each agent under the terminal that started it.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
};

use super::render::{display_width, put_segment, put_text};
use super::*;
use crate::protocol::{ClientShellAgent, ClientShellAgentSpawner};

/// Panel width, including its separator column.
pub(super) const AGENT_TEAMS_WIDTH: u16 = 30;
/// Width the tab bar and panes keep; narrower terminals drop the panel.
pub(super) const AGENT_TEAMS_MIN_MAIN_WIDTH: u16 = 40;
const HEADER_ROWS: u16 = 2;

pub(super) fn has_agent_teams(snapshot: &ClientShellSnapshot) -> bool {
    snapshot
        .agents
        .iter()
        .any(|agent| agent.spawned_by.is_some())
}

pub(super) struct AgentTeam<'a> {
    pub(super) spawner: &'a ClientShellAgentSpawner,
    pub(super) workers: Vec<&'a ClientShellAgent>,
}

/// Groups agents by spawning terminal in spawner pane order. Teams whose
/// spawner pane has closed follow, and workers keep snapshot agent order.
pub(super) fn agent_teams(snapshot: &ClientShellSnapshot) -> Vec<AgentTeam<'_>> {
    let mut teams: Vec<AgentTeam<'_>> = Vec::new();
    for agent in &snapshot.agents {
        let Some(spawner) = agent.spawned_by.as_ref() else {
            continue;
        };
        match teams
            .iter_mut()
            .find(|team| team.spawner.terminal_id == spawner.terminal_id)
        {
            Some(team) => team.workers.push(agent),
            None => teams.push(AgentTeam {
                spawner,
                workers: vec![agent],
            }),
        }
    }
    teams.sort_by_key(|team| {
        team.spawner
            .pane_id
            .as_deref()
            .and_then(|pane_id| {
                snapshot
                    .panes
                    .iter()
                    .position(|pane| pane.pane_id == pane_id)
            })
            .unwrap_or(usize::MAX)
    });
    teams
}

enum TeamLine<'a> {
    Spawner {
        label: &'a str,
        status: Option<crate::api::schema::AgentStatus>,
        /// Set only while the spawner pane is open, so its row can focus it.
        pane_id: Option<&'a str>,
        focused: bool,
    },
    Worker {
        agent: &'a ClientShellAgent,
        last: bool,
    },
    Gap,
}

impl TeamLine<'_> {
    fn pane_id(&self) -> Option<&str> {
        match self {
            Self::Spawner { pane_id, .. } => *pane_id,
            Self::Worker { agent, .. } => Some(&agent.pane_id),
            Self::Gap => None,
        }
    }
}

fn agent_label(agent: &ClientShellAgent) -> Option<&str> {
    agent
        .name
        .as_deref()
        .or(agent.display_agent.as_deref())
        .or(agent.agent.as_deref())
        .or(agent.title.as_deref())
}

fn team_lines<'a>(snapshot: &'a ClientShellSnapshot, teams: &[AgentTeam<'a>]) -> Vec<TeamLine<'a>> {
    let mut lines = Vec::new();
    for (index, team) in teams.iter().enumerate() {
        if index > 0 {
            lines.push(TeamLine::Gap);
        }
        let spawner_pane_id = team.spawner.pane_id.as_deref();
        let pane = spawner_pane_id
            .and_then(|pane_id| snapshot.panes.iter().find(|pane| pane.pane_id == pane_id));
        let agent = spawner_pane_id.and_then(|pane_id| {
            snapshot
                .agents
                .iter()
                .find(|agent| agent.pane_id == pane_id)
        });
        let label = agent
            .and_then(agent_label)
            .or(team.spawner.name.as_deref())
            .or(pane.and_then(|pane| pane.label.as_deref()))
            .or(spawner_pane_id)
            .unwrap_or("closed pane");
        lines.push(TeamLine::Spawner {
            label,
            status: agent.map(|agent| agent.agent_status),
            pane_id: pane.map(|pane| pane.pane_id.as_str()),
            focused: agent
                .map(|agent| agent.focused)
                .or(pane.map(|pane| pane.focused))
                .unwrap_or(false),
        });
        let last = team.workers.len().saturating_sub(1);
        lines.extend(
            team.workers
                .iter()
                .enumerate()
                .map(|(index, agent)| TeamLine::Worker {
                    agent,
                    last: index == last,
                }),
        );
    }
    lines
}

pub(super) fn render_agent_teams(
    buffer: &mut Buffer,
    area: Rect,
    snapshot: &ClientShellSnapshot,
    config: &ClientShellConfig,
    hits: &mut ShellHitMap,
) {
    let palette = &config.palette;
    buffer.set_style(area, Style::default().bg(palette.sidebar_bg));
    for y in area.y..area.bottom() {
        if let Some(cell) = buffer.cell_mut((area.x, y)) {
            cell.set_symbol("│");
            cell.set_style(Style::default().fg(palette.surface_dim));
        }
    }
    let inner = Rect::new(
        area.x.saturating_add(1),
        area.y,
        area.width.saturating_sub(1),
        area.height,
    );
    if inner.is_empty() {
        return;
    }
    let header = Style::default()
        .fg(palette.overlay0)
        .add_modifier(Modifier::BOLD);
    put_text(
        buffer,
        inner.x,
        inner.y,
        inner.width,
        " agent teams",
        header,
    );
    if config.mouse_capture && inner.width > 2 {
        let close = Rect::new(inner.right().saturating_sub(2), inner.y, 1, 1);
        put_text(buffer, close.x, close.y, 1, "×", header);
        hits.agent_teams_close = close;
    }

    let body = Rect::new(
        inner.x,
        inner.y.saturating_add(HEADER_ROWS),
        inner.width,
        inner.height.saturating_sub(HEADER_ROWS),
    );
    let lines = team_lines(snapshot, &agent_teams(snapshot));
    let capacity = body.height as usize;
    let shown = if lines.len() > capacity {
        capacity.saturating_sub(1)
    } else {
        lines.len()
    };
    for (index, line) in lines.iter().take(shown).enumerate() {
        let rect = Rect::new(body.x, body.y + index as u16, body.width, 1);
        render_team_line(buffer, rect, line, config);
        if let Some(pane_id) = line.pane_id() {
            hits.agents.push((rect, pane_id.to_owned()));
        }
    }
    let hidden_workers = lines[shown..]
        .iter()
        .filter(|line| matches!(line, TeamLine::Worker { .. }))
        .count();
    if hidden_workers > 0 && shown < capacity {
        put_text(
            buffer,
            body.x,
            body.y + shown as u16,
            body.width,
            &format!(" +{hidden_workers} more"),
            Style::default()
                .fg(palette.overlay0)
                .add_modifier(Modifier::DIM),
        );
    }
}

fn render_team_line(
    buffer: &mut Buffer,
    rect: Rect,
    line: &TeamLine<'_>,
    config: &ClientShellConfig,
) {
    let palette = &config.palette;
    let focused = match line {
        TeamLine::Spawner { focused, .. } => *focused,
        TeamLine::Worker { agent, .. } => agent.focused,
        TeamLine::Gap => return,
    };
    if focused {
        buffer.set_style(rect, Style::default().bg(palette.active_row_bg));
    }
    let label_style = Style::default()
        .fg(if focused {
            palette.text
        } else {
            palette.subtext0
        })
        .add_modifier(Modifier::BOLD);
    let right = rect.right();
    let mut x = rect.x.saturating_add(1);
    let (label, status, status_label) = match line {
        TeamLine::Spawner { label, status, .. } => (*label, *status, None),
        TeamLine::Worker { agent, last } => {
            let branch = if *last { "└ " } else { "├ " };
            x = put_segment(
                buffer,
                x.saturating_add(1),
                rect.y,
                right,
                branch,
                Style::default().fg(palette.surface_dim),
            );
            let status_label = agent
                .state_labels
                .iter()
                .find(|(status, _)| status == status_text(agent.agent_status))
                .map(|(_, label)| label.as_str())
                .unwrap_or_else(|| status_text(agent.agent_status));
            (
                agent_label(agent).unwrap_or(agent.pane_id.as_str()),
                Some(agent.agent_status),
                Some(status_label),
            )
        }
        TeamLine::Gap => return,
    };
    let (icon, icon_style) = match status {
        Some(status) => (
            status_icon(status, config.status_indicators),
            Style::default().fg(status_color(status, palette)),
        ),
        None => ("▸", Style::default().fg(palette.overlay0)),
    };
    x = put_segment(buffer, x, rect.y, right, icon, icon_style);
    x = x.saturating_add(1);
    let status_x = match (status, status_label) {
        (Some(status), Some(status_label)) => {
            let width = display_width(status_label);
            let status_x = right.saturating_sub(width.saturating_add(1));
            if status_x > x.saturating_add(1) {
                put_text(
                    buffer,
                    status_x,
                    rect.y,
                    width,
                    status_label,
                    Style::default().fg(status_color(status, palette)),
                );
                status_x
            } else {
                right
            }
        }
        _ => right,
    };
    put_text(
        buffer,
        x,
        rect.y,
        status_x.saturating_sub(x).saturating_sub(1),
        label,
        label_style,
    );
}
