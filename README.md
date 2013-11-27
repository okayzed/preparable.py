# data dispatching done wrong

This is a poor imitation of the FB's data dispatch mechanism (aka Preparables).

It allows concurrent data dispatches to run in parallel and exposes the data to
multi-step functions in a mostly transparent manner.

[ Read More About Preparables ](http://www.quora.com/Facebook-Infrastructure/What-are-preparables-and-how-are-they-implemented )

## Usage

A preparable is a function that yields long running synchronous functions
to be run in a separate thread. This allows multiple dispatches to happen
in parallel with python multiprocesses, while the code still looks linear.

See examples/ directory for how they are used

## Caveats

Functions that are yielded must be visible from your module top level, because
they have to be marshalled across processes. This means no lambdas! Instead,
use module level named functions.

## What's not Implemented

* Child Preparables
* Errors (partially implemented, fully undocumented)
