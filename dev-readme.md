## Cooperative Multi-Concurrency

We often rely upon the mechanism of cooperative multi-concurrency to avoid race conditions. Namely, in an async
function, control is only yielded back at `await` points. This means that we can safely mutate shared state with
blocking operations in a threadsafe manner. However, this makes it more difficult for new developers to understand the
code. For example, if a developer is not aware of the cooperative nature of the concurrency, they may write code that
uses `await` in a section that relies on control not being yielded back. Therefore, we try to comment where extra
caution is needed.