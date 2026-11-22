// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// TODO-RAINCLAUDE: Taking these function pointers makes this crate compile
// the future's poll and drop glue, without calling or boxing anything.
#[inline(never)]
fn anchor_future<Fut: Future>() {
    let poll: fn(Pin<&mut Fut>, &mut Context<'_>) -> Poll<Fut::Output> =
        <Fut as Future>::poll;
    let drop: unsafe fn(*mut Fut) = std::ptr::drop_in_place::<Fut>;
    std::hint::black_box((poll, drop));
}

macro_rules! anchor_async_fn_impls {
    ($($name:ident($($arg:ident),*);)*) => {
        $(
            #[inline(never)]
            pub fn $name<F, Fut, $($arg),*>(_f: F)
            where
                F: Fn($($arg),*) -> Fut,
                Fut: Future,
            {
                anchor_future::<Fut>();
            }
        )*
    };
}

anchor_async_fn_impls! {
    anchor_async_fn_0();
    anchor_async_fn_1(A1);
    anchor_async_fn_2(A1, A2);
    anchor_async_fn_3(A1, A2, A3);
    anchor_async_fn_4(A1, A2, A3, A4);
    anchor_async_fn_5(A1, A2, A3, A4, A5);
    anchor_async_fn_6(A1, A2, A3, A4, A5, A6);
    anchor_async_fn_7(A1, A2, A3, A4, A5, A6, A7);
    anchor_async_fn_8(A1, A2, A3, A4, A5, A6, A7, A8);
    anchor_async_fn_9(A1, A2, A3, A4, A5, A6, A7, A8, A9);
    anchor_async_fn_10(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10);
    anchor_async_fn_11(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11);
    anchor_async_fn_12(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12);
}
