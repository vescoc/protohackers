# Protohackers challenges you to create servers for network protocols.

We give you the protocol spec. You write the server and host it. We
automatically test it. There's a global leaderboard for the fastest
solve times.

## Rust and Rust/WASI solutions

You can find the Rust solutions on subfolder `rust` and the Rust/WASI
solutions, based on a custom made async runtime tokio-like, on
subfolder `wasi`.

For running the WASI solutions, for example for running the tests,
install [wasmtime](https://github.com/bytecodealliance/wasmtime). For
example:

```shell
curl https://wasmtime.dev/install.sh -sSf | bash
```

For compiling Rust/WASI you must install the target `wasm32-wasip2`

```shell
rustup target add wasm32-wasip2
```

## Running a server

In `rust` or `wasi` subfolder, run `cargo` like this:

> cargo run -r -p p00-smoke-test

The server listen by default on port `10000` for any incoming address.

## The solutions

1. `p00-smoke-test` [00 - Smoke Test](https://protohackers.com/problem/0), implementation in [Rust](rust/p00-smoke-test) and [Rust/WASI](wasi/problems/p00-smoke-test)
2. `p01-prime-time` [01 - Prime Time](https://protohackers.com/problem/1), implementation in [Rust](rust/p01-prime-time) and [Rust/WASI](wasi/problems/p01-prime-time)
3. `p02-means-to-an-end` [02 - Means To An End](https://protohackers.com/problem/2), implementation in [Rust](rust/p02-means-to-an-end) and [Rust/WASI](wasi/problems/p02-means-to-an-end)
4. `p03-budget-chat` [03 - Budget Chat](https://protohackers.com/problem/3), implementation in [Rust](rust/p03-budget-chat) and [Rust/WASI](wasi/problems/p03-budget-chat)
5. `p04-unusual-database-program` [04 - Unusual Database Program](https://protohackers.com/problem/4), implementation in [Rust](rust/p04-unusual-database-program) and [Rust/WASI](wasi/problems/p04-unusual-database-program)
6. `p05-mob-in-the-middle` [05 Mob In The Middle](https://protohackers.com/problem/5), implementation in [Rust](rust/p05-mob-in-the-middle) and [Rust/WASI](wasi/problems/p05-mob-in-the-middle)
7. `p06-speed-daemon` [06 - Speed Daemon](https://protohackers.com/problem/6), implementation in [Rust](rust/p06-speed-daemon) and [Rust/WASI](wasi/problems/p06-speed-daemon)
8. `p07-line-reversal` [07 - Line Reversal](https://protohackers.com/problem/7), implementation in [Rust](rust/p08-insecure-sockets-layer) and [Rust/WASI](wasi/problems/p08-insecure-sockets-layer)
9. `p08-insecure-sockets-layer` [08 - Insecure Sockets Layer](https://protohackers.com/problem/8), implementation in [Rust](rust/p08-insecure-sockets-layer) and [Rust/WASI](wasi/problems/p08-insecure-sockets-layer)
10. `p09-job-centre` [09 Job Centre](https://protohackers.com/problem/9), implementation in [Rust](rust/p09-job-centre) and [Rust/WASI](wasi/problems/p09-job-centre)
11. `p10-voracious-code-storage` [10 - Voracious Code Storage](https://protohackers.com/problem/10), implementation in [Rust](rust/p10-voracious-code-storage) and [Rust/WASI](wasi/problems/p10-voracious-code-storage)
12. `p11-pest-control` [11 Pest Control](https://protohackers.com/problem/11), implementation in [Rust](rust/p11-pest-control) and [Rust/WASI](wasi/problems/p11-pest-control)
