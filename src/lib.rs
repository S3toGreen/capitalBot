mod com_sink;
mod com_events;

use pyo3::prelude::*;
use windows::core::{*};
use windows::Win32::System::Com::{
    IConnectionPoint, IConnectionPointContainer
};
use crate::com_sink::{DomesticSink,_ISKQuoteLibEvents, ISKQuoteLib};
use crate::com_events::dispids::DMHandler;

pub unsafe fn advise_from_raw_iunknown(
    raw_ptr: usize,
) -> Result<()> {
    // advise event interface and store the ISKLib interface

    let ptr = raw_ptr as *mut std::ffi::c_void;
    let unk = IUnknown::from_raw_borrowed(&ptr).unwrap();

    let cpc: IConnectionPointContainer = unk.cast()?;
    let iid = &<_ISKQuoteLibEvents as Interface>::IID;
    let cp: IConnectionPoint = cpc.FindConnectionPoint(iid)?;

    let com:ISKQuoteLib = unk.cast()?;
    // com.Invoke(33, riid, lcid, wflags, pdispparams, pvarresult, pexcepinfo, puargerr)
    // Create sink COM object
    let sink = DomesticSink::new(DMHandler);
    // let sink_dispatch: IDispatch = sink.into();
    let sink_unknown: IUnknown = sink.into();

    // // let mut cookie: u32 = 0;
    cp.Advise(&sink_unknown)?;

    Ok(())
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn register_sink(raw_ptr: usize)->PyResult<()>{
    // log::info!("register_sink:{}", raw_ptr);
    println!("rust addr:{}", raw_ptr);
    unsafe {
        advise_from_raw_iunknown(raw_ptr).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Advise failed: {e:?}")))?
    }
    Ok(())
}
#[pyfunction]
fn unregister_sink()->PyResult<()>{
    // log::info!("unregister_sink called");
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(register_sink, m)?)?;
    m.add_function(wrap_pyfunction!(unregister_sink, m)?)?;
    Ok(())
}
