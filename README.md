# data dispatching done wrong

This is a poor imitation of the FB's data dispatch mechanism (aka Preparables).

It allows concurrent data dispatches to run in parallel and exposes the data to
multi-step functions in a mostly transparent manner.

## Usage

See examples/ directory

## Caveats

Functions that are yielded must be visible from your module top level, because
they have to be marshalled across processes. This means no lambdas! Instead,
use module level named functions.
