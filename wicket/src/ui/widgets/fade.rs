use crate::ui::defaults::style;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;

/// Style every character as `style::faded_background`
#[derive(Default)]
pub struct Fade {}

impl Widget for Fade {
    fn render(self, area: Rect, buf: &mut Buffer) {
        buf.set_style(area, style::faded_background());
    }
}
