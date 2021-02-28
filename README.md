streaming-cassava
=================

[![Hackage](https://img.shields.io/hackage/v/streaming-cassava.svg)](https://hackage.haskell.org/package/streaming-cassava) [![Build Status](https://github.com/haskell-streaming/streaming-cassava/actions/workflows/haskell-ci.yml/badge.svg)](https://github.com/haskell-streaming/streaming-cassava/actions/workflows/haskell-ci.yml)

> [cassava] support for the [streaming] ecosystem

[cassava]: http://hackage.haskell.org/package/cassava
[streaming]: http://hackage.haskell.org/package/streaming

This library allows you to easily stream CSV data in and out.  You can
do so using both "plain" record-based (with optional header support)
or name-based (header required to determine ordering)
encoding/decoding.

All encoding/decoding options are supported, it's possible to
automatically add on default headers and you can even choose whether
to fail on the first parse error or handle errors on a row-by-row
basis.

Reading data from file
----------------------

A common use-case is to stream CSV-encoded data in from a file.  You
may be tempted to use `readFile` from [streaming-bytestring] to obtain
the file contents, and for simple cases this should suffice.  However,
the recommended solution is to use the [streaming-with] package for
the IO aspects.  You can then write something like:

```haskell
withBinaryFileContents \"myFile.csv\" $
  doSomethingWithStreamingCSV
  . 'decodeByName'
```

[streaming-bytestring]: https://hackage.haskell.org/package/streaming-bytestring
[streaming-with]: https://hackage.haskell.org/package/streaming-with
