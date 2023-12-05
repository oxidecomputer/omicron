use crate::ui::defaults::style;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;

/// Style every character as `style::faded_background`
#[derive(Default)]
pub struct Fade {}

impl Widget for Fade {
    fn render(self, area: Rect, buf: &mut Buffer) {
        for x in area.left()..area.right() {
            for y in area.top()..area.bottom() {
                buf.set_string(
                    x,
                    y,
                    buf.get(x, y).symbol.clone(),
                    style::faded_background(),
                );
            }
        }
    }
}
