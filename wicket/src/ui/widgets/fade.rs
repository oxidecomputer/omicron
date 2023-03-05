use crate::ui::defaults::style;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::widgets::Widget;

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
