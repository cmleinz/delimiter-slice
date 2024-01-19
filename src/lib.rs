//! Stream type for splitting a delimited stream  
//! ```rust
//! # use tokio_util::io::ReaderStream;
//! # use delimiter_slice::DelimiterSlice;
//! # use futures_util::StreamExt;
//! # #[tokio::main]
//! # async fn main() {
//! const TEST: &[u8] = b"FOOBARFOOBARBAZFOO";
//! const DELIM: &[u8] = b"BAZ";
//!
//! let stream = ReaderStream::new(TEST);
//! let mut slice_stream = DelimiterSlice::new(stream, DELIM);
//! let data = slice_stream.next().await.unwrap().unwrap();
//! assert_eq!(&data, &TEST[0..12]);
//! let data = slice_stream.next().await.unwrap().unwrap();
//! assert_eq!(&data, &TEST[15..]);
//! # }
//! ```
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes, BytesMut};
use futures_core::{ready, Stream};
use pin_project_lite::pin_project;

pin_project! {
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DelimiterSlice<St, D> {
        #[pin]
        stream: St,
    buf: BytesMut,
        delimiter: D,
    found: bool,
    limit: usize,
    }
}

impl<St, D> DelimiterSlice<St, D> {
    const CAPACITY: usize = 8_192;

    const LIMIT: usize = usize::MAX;

    /// Create a new `DelimiterSlice` based on the provided stream and delimiter.
    ///
    /// This defaults instantiating the underlying buffer with a capacity of 8,192 bytes.
    pub fn new(stream: St, delimiter: D) -> Self {
        Self::with_capacity_and_limit(stream, delimiter, Self::CAPACITY, Self::LIMIT)
    }

    pub fn with_capacity(stream: St, delimiter: D, capacity: usize) -> Self {
        Self::with_capacity_and_limit(stream, delimiter, capacity, Self::LIMIT)
    }

    pub fn with_limit(stream: St, delimiter: D, limit: usize) -> Self {
        Self::with_capacity_and_limit(stream, delimiter, Self::CAPACITY, limit)
    }

    pub fn with_capacity_and_limit(
        stream: St,
        delimiter: D,
        capacity: usize,
        limit: usize,
    ) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(capacity),
            delimiter,
            found: false,
            limit,
        }
    }

    /// Return the wrapped stream.
    ///
    /// This is useful once the delimiter has been returned and the internal buffer has been cleared
    /// by calling `next()` again.
    ///
    /// # Panics
    ///
    /// Panics if the internal buffer is not empty when this is called. The stated purpose of this
    /// library is to provide a simple and safe way to extract data from a delimited stream, but
    /// allow the stream to continue producing the data in the order it was received.
    ///
    /// If you've called into_inner before a second call to `next` this is likely an error.
    pub fn into_inner(self) -> St {
        assert!(self.buf.is_empty());
        self.stream
    }

    fn limit_reached_err() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::OutOfMemory, "Limit exceeded")
    }
}

impl<St, D> Stream for DelimiterSlice<St, D>
where
    D: AsRef<[u8]>,
    St: Stream<Item = Result<Bytes, std::io::Error>>,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<St::Item>> {
        let mut this = self.project();

        if *this.found {
            if this.buf.is_empty() {
                return this.stream.poll_next(cx);
            } else {
                return Poll::Ready(Some(Ok(this.buf.split().freeze())));
            }
        }

        let mut exhausted = false;

        loop {
            let delim = this.delimiter.as_ref();
            if let Some(index) = this
                .buf
                .windows(delim.len())
                .position(|window| window.eq(delim))
            {
                let data = this.buf.split_to(index).freeze();
                this.buf.advance(delim.len());
                *this.found = true;
                return Poll::Ready(Some(Ok(data)));
            }

            if exhausted {
                return Poll::Ready(None);
            }

            match ready!(this.stream.as_mut().poll_next(cx)) {
                Some(Ok(data)) => {
                    if this.buf.len().saturating_add(data.len()).ge(this.limit) {
                        return Poll::Ready(Some(Err(Self::limit_reached_err())));
                    } else {
                        this.buf.extend_from_slice(&data);
                        continue;
                    }
                }
                Some(error) => return Poll::Ready(Some(error)),
                None => exhausted = true,
            }
        }
    }
}
