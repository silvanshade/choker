pub(crate) struct BlockingReceiver<T> {
    rx: T,
}

impl<T> BlockingReceiver<T> {
    pub(crate) fn new(rx: T) -> Self {
        Self { rx }
    }
}

pub(crate) trait BlockingReceivable<T> {
    type Item;
    fn blocking_recv(&mut self) -> Option<Self::Item>;
}

impl<T> BlockingReceivable<tokio::sync::mpsc::UnboundedReceiver<T>>
    for BlockingReceiver<tokio::sync::mpsc::UnboundedReceiver<T>>
{
    type Item = T;

    #[inline]
    fn blocking_recv(&mut self) -> Option<Self::Item> {
        self.rx.blocking_recv()
    }
}

impl<T> Iterator for BlockingReceiver<T>
where
    BlockingReceiver<T>: BlockingReceivable<T>,
{
    type Item = <Self as BlockingReceivable<T>>::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        BlockingReceiver::blocking_recv(self)
    }
}

// struct TokioMpscReceiverIterator<T> {
//     rx: tokio::sync::mpsc::UnboundedReceiver<T>,
// }

// impl<T> TokioMpscReceiverIterator<T> {
//     fn new(rx: tokio::sync::mpsc::UnboundedReceiver<T>) -> Self {
//         Self { rx }
//     }
// }

// impl<T> Iterator for TokioMpscReceiverIterator<T> {
//     type Item = T;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.rx.blocking_recv()
//     }
// }
