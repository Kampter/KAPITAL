use numpy::PyArray1;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Internal generic ring buffer used by the exposed PyO3 classes.
#[derive(Clone, Debug)]
struct RingBuffer<T>
where
    T: Copy + Default,
{
    data: Vec<T>,
    capacity: usize,
    len: usize,
    cursor: usize,
}

impl<T> RingBuffer<T>
where
    T: Copy + Default,
{
    fn new(capacity: usize) -> Result<Self, PyErr> {
        if capacity == 0 {
            return Err(PyValueError::new_err("capacity must be greater than zero"));
        }
        Ok(Self {
            data: vec![T::default(); capacity],
            capacity,
            len: 0,
            cursor: 0,
        })
    }

    fn push(&mut self, value: T) {
        self.data[self.cursor] = value;
        self.cursor += 1;
        if self.cursor == self.capacity {
            self.cursor = 0;
        }
        if self.len < self.capacity {
            self.len += 1;
        }
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        for &value in values {
            self.push(value);
        }
    }

    fn clear(&mut self) {
        self.len = 0;
        self.cursor = 0;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    fn snapshot_vec(&self) -> Vec<T> {
        let mut out = Vec::with_capacity(self.len);
        if self.len == 0 {
            return out;
        }
        let start = if self.len == self.capacity {
            self.cursor
        } else {
            0
        };
        for idx in 0..self.len {
            let position = (start + idx) % self.capacity;
            out.push(self.data[position]);
        }
        out
    }
}

#[pyclass(module = "kapital_rust", name = "RingBufferI64")]
pub struct RingBufferI64 {
    inner: RingBuffer<i64>,
}

#[pymethods]
impl RingBufferI64 {
    #[new]
    pub fn new(capacity: usize) -> PyResult<Self> {
        Ok(Self {
            inner: RingBuffer::new(capacity)?,
        })
    }

    pub fn push(&mut self, value: i64) {
        self.inner.push(value);
    }

    pub fn extend(&mut self, values: Vec<i64>) {
        self.inner.extend_from_slice(&values);
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[pyo3(name = "__len__")]
    fn py_len(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    #[getter]
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    pub fn snapshot(&self) -> Vec<i64> {
        self.inner.snapshot_vec()
    }

    pub fn iter(&self) -> Vec<i64> {
        self.inner.snapshot_vec()
    }

    #[allow(deprecated)]
    pub fn to_numpy(&self, py: Python<'_>) -> PyResult<Py<PyArray1<i64>>> {
        let snapshot = self.inner.snapshot_vec();
        Ok(PyArray1::from_vec(py, snapshot).to_owned())
    }
}

#[pyclass(module = "kapital_rust", name = "RingBufferF64")]
pub struct RingBufferF64 {
    inner: RingBuffer<f64>,
}

#[pymethods]
impl RingBufferF64 {
    #[new]
    pub fn new(capacity: usize) -> PyResult<Self> {
        Ok(Self {
            inner: RingBuffer::new(capacity)?,
        })
    }

    pub fn push(&mut self, value: f64) {
        self.inner.push(value);
    }

    pub fn extend(&mut self, values: Vec<f64>) {
        self.inner.extend_from_slice(&values);
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[pyo3(name = "__len__")]
    fn py_len(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    #[getter]
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    pub fn snapshot(&self) -> Vec<f64> {
        self.inner.snapshot_vec()
    }

    pub fn iter(&self) -> Vec<f64> {
        self.inner.snapshot_vec()
    }

    #[allow(deprecated)]
    pub fn to_numpy(&self, py: Python<'_>) -> PyResult<Py<PyArray1<f64>>> {
        let snapshot = self.inner.snapshot_vec();
        Ok(PyArray1::from_vec(py, snapshot).to_owned())
    }
}

#[pymodule]
fn kapital_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RingBufferI64>()?;
    m.add_class::<RingBufferF64>()?;
    Ok(())
}
