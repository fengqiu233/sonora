use std::rc::Rc;

use gpui::{App, Entity, IntoElement, RenderOnce, Window, div, prelude::*, px};
use i18n::{lookup, t};
use ui::{ActiveTheme as _, Button, Input, Modal, Text};

use crate::shared::steps::steps;

type Action = Rc<dyn Fn(&(), &mut Window, &mut App)>;

/// The walkthrough for one provider's manual cookie sign-in: the page to send the user to
/// and the keys of every string the dialog shows, so one dialog serves every cookie provider.
struct Guide {
    url: &'static str,
    open: &'static str,
    title: &'static str,
    hint: &'static str,
    steps: [&'static str; 4],
    note: &'static str,
}

const YOUTUBE: Guide = Guide {
    url: "https://music.youtube.com",
    open: "login-cookie-open",
    title: "login-cookie-title",
    hint: "login-cookie-hint",
    steps: [
        "login-cookie-step-1",
        "login-cookie-step-2",
        "login-cookie-step-3",
        "login-cookie-step-4",
    ],
    note: "login-cookie-step-note",
};

const DEEZER: Guide = Guide {
    url: "https://www.deezer.com",
    open: "login-cookie-deezer-open",
    title: "login-cookie-deezer-title",
    hint: "login-cookie-deezer-hint",
    steps: [
        "login-cookie-deezer-step-1",
        "login-cookie-deezer-step-2",
        "login-cookie-deezer-step-3",
        "login-cookie-deezer-step-4",
    ],
    note: "login-cookie-deezer-note",
};

/// The guide for a provider slug. YouTube's is the fallback, since it was the only cookie
/// provider before Deezer and its wording fits any header paste.
fn guide(slug: &str) -> &'static Guide {
    match slug {
        "deezer" => &DEEZER,
        _ => &YOUTUBE,
    }
}

#[derive(IntoElement)]
pub(crate) struct CookiePrompt {
    slug: &'static str,
    secret: Entity<Input>,
    submit: Option<Action>,
    cancel: Option<Action>,
}

impl CookiePrompt {
    pub(crate) fn new(slug: &'static str, secret: Entity<Input>) -> Self {
        Self {
            slug,
            secret,
            submit: None,
            cancel: None,
        }
    }

    /// The hint key the paste field should carry for a provider, set on the input when the
    /// manual sign-in starts so it follows the language like every other hint.
    pub(crate) fn hint(slug: &str) -> &'static str {
        guide(slug).hint
    }

    pub(crate) fn on_submit(
        mut self,
        handler: impl Fn(&(), &mut Window, &mut App) + 'static,
    ) -> Self {
        self.submit = Some(Rc::new(handler));
        self
    }

    pub(crate) fn on_cancel(
        mut self,
        handler: impl Fn(&(), &mut Window, &mut App) + 'static,
    ) -> Self {
        self.cancel = Some(Rc::new(handler));
        self
    }
}

impl RenderOnce for CookiePrompt {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let Self {
            slug,
            secret,
            submit,
            cancel,
        } = self;
        let dismissed = cancel.clone();
        let theme = *cx.theme();
        let guide = guide(slug);

        Modal::new("cookie-prompt", lookup(guide.title, None))
            .w(px(560.))
            .child(
                Button::new("open-cookie-provider")
                    .label(lookup(guide.open, None))
                    .icon("icons/external-link.svg")
                    .outline()
                    .on_click(move |_, _, cx| cx.open_url(guide.url)),
            )
            .child(steps(guide.steps.iter().map(|key| lookup(key, None))))
            .child(
                div()
                    .child(lookup(guide.note, None))
                    .flex_1()
                    .min_w_0()
                    .text_size(theme.text(Text::Small))
                    .text_color(theme.muted_foreground),
            )
            .child(secret)
            .action(
                Button::new("cancel-cookies")
                    .ghost()
                    .label(t!("common-cancel"))
                    .on_click(move |_, window, cx| {
                        if let Some(cancel) = &cancel {
                            cancel(&(), window, cx);
                        }
                    }),
            )
            .action(
                Button::new("submit-cookies")
                    .label(t!("login-cookie-submit"))
                    .primary()
                    .on_click(move |_, window, cx| {
                        if let Some(submit) = &submit {
                            submit(&(), window, cx);
                        }
                    }),
            )
            .on_dismiss(move |_, window, cx| {
                if let Some(dismissed) = &dismissed {
                    dismissed(&(), window, cx);
                }
            })
    }
}
