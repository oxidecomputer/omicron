/// A macro intended to reduce boilerplate when writing saga actions.
///
/// This macro aims to reduce this boilerplate, by requiring only the following:
/// - The name of the saga
/// - The name of each action
/// - The output of each action
/// - The "forward" action function
/// - (Optional) The "undo" action function
///
/// For this input:
///
/// ```ignore
/// declare_saga_actions! {
///     my_saga;
///     SAGA_NODE1 -> "output1" {
///         + do1
///         - undo1
///     }
///     SAGA_NODE2 -> "output2" {
///         + do2
///     }
/// }
/// ```
///
/// We generate the following:
/// - For `SAGA_NODE1`:
///     - A `NexusAction` labeled "my_saga.saga_node1" (containing "do1" and "undo1").
///     - `fn saga_node1_action() -> steno::Node` referencing this node, with an
///     output named "output1".
/// - For `SAGA_NODE2`:
///     - A `NexusAction` labeled "my_saga.saga_node2" (containing "do2").
///     - `fn saga_node2_action() -> steno::Node` referencing this node, with an
///     output named "output2".
/// - For `my_saga`:
///     - `fn my_saga_register_actions(...)`, which can be called to implement
///     `NexusSaga::register_actions`.
#[macro_export]
macro_rules! declare_saga_actions {
    // The entrypoint to the macro.
    // We expect the input to be of the form:
    //
    //  saga-name;
    ($saga:ident; $($tail:tt)*) => {
        declare_saga_actions!(S = $saga <> $($tail)*);
    };
    // Subsequent lines of the saga action declaration.
    // These take the form:
    //
    //  ACTION_NAME -> "output" {
    //      + action
    //      - undo_action
    //  }
    //
    // However, we also want to propagate the Saga structure and collection of
    // all node names, so this is *actually* parsed with a hidden prefix:
    //
    //  S = SagaName <old nodes> <> ...
    //
    // Basically, everything to the left of "<>" is just us propagating state
    // through the macro, and everything to the right of it is user input.
    (S = $saga:ident $($nodes:ident),* <> $node:ident -> $out:literal { + $a:ident - $u:ident } $($tail:tt)*) => {
        static $node: ::std::sync::LazyLock<$crate::NexusAction> =
            ::std::sync::LazyLock::new(|| {
                $crate::macro_support::steno::ActionFunc::new_action(
                    $crate::__action_name!($saga, $node), $a, $u,
                )
            });
        $crate::__emit_action!($node, $out);
        declare_saga_actions!(S = $saga $($nodes,)* $node <> $($tail)*);
    };
    // Same as the prior match, but without the undo action.
    (S = $saga:ident $($nodes:ident),* <> $node:ident -> $out:literal { + $a:ident } $($tail:tt)*) => {
        static $node: ::std::sync::LazyLock<$crate::NexusAction> =
            ::std::sync::LazyLock::new(|| {
                ::steno::new_action_noop_undo(
                    $crate::__action_name!($saga, $node), $a,
                )
            });
        $crate::__emit_action!($node, $out);
        declare_saga_actions!(S = $saga $($nodes,)* $node <> $($tail)*);
    };
    // The end of the macro, which registers all previous generated saga nodes.
    //
    // We generate a new function, rather than implementing
    // "NexusSaga::register_actions", because traits cannot be partially
    // implemented, and "make_saga_dag" is not being generated through this
    // macro.
    (S = $saga:ident $($nodes:ident),* <>) => {
        $crate::macro_support::paste::paste! {
            fn [<$saga _list_actions>]() -> Vec<$crate::NexusAction> {
                vec![
                    $(
                        ::std::sync::Arc::clone(&* $nodes ),
                    )*
                ]
            }
        }
    };
}

#[macro_export]
macro_rules! __emit_action {
    ($node:ident, $output:literal) => {
        $crate::macro_support::paste::paste! {
            #[allow(dead_code)]
            fn [<$node:lower _action>]() -> $crate::macro_support::steno::Node {
                $crate::macro_support::steno::Node::action(
                    $output,
                    $crate::__stringify_ident!([<$node:camel>]),
                    $node.as_ref(),
                )
            }
        }
    };
}

#[macro_export]
macro_rules! __action_name {
    ($saga:ident, $node:ident) => {
        $crate::macro_support::paste::paste! {
            concat!(
                stringify!($saga),
                ".",
                $crate::__stringify_ident!([<$node:lower>]),
            )
        }
    };
}

#[macro_export]
macro_rules! __stringify_ident {
    ($i:ident) => {
        stringify!($i)
    };
}
