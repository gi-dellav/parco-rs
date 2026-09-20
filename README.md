# disco-rs ~ Distributed Computing for Rust

*disco-rs* is a Rust framework that allows you to build software that execute computations across multiple machines using authoritative network.

Each network is split into Leaders and Workers, which each Leader managing informations and each Worker managing calculations; at the same time, Parco allows for flexible networks, with Leaders that send messages to other Leaders for better scaling, Workers being connected to multiple Leaders, and having Workers and Leaders coexist in the same container.

DiscoRS follows a crate-splitting design, with the framework being divided into:
- *disco-foundation* for defining the data structures and traits shared between all of other Disco libraries
- *disco-leader* for implementing the logic that runs on Leader nodes
- *disco-worker* for implementing the logic that runs on Worker nodes
- (optional) *disco-tester* for running the autonomous end-to-end tests in a simulated environment

Users must then split their projects into two crates, one to produce binaries running on Leader nodes and one for Worker nodes.

This project also ships `examples/multiplicator`, a simple network for running incremental multiplications (so `value = value * input`) on large input vectors.
