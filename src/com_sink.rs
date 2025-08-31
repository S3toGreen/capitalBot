use windows::core::*;
use windows::Win32::System::Com::*;
use windows::Win32::System::Variant::*;
use windows::Win32::Foundation::E_NOTIMPL;
use super::com_events::dispids::{
    dispatch_domesticevents, dispatch_overseaevents, DomesticEvents, OverseaEvents,
};
/// Outgoing events IID from your comtypes _ISKQuoteLibEvents
// const IID_QUOTE_LIB_EVENTS: GUID = GUID::from_u128(0xB6B30C9A_CA64_437B_B415_3BD03004A544);
// const IID_OS_QUOTE_LIB_EVENTS: GUID = GUID::from_u128(0x8DB2F1B3_20D6_4EC3_9614_CC6154B6FF72);

// QuoteEvent GUID
#[interface("B6B30C9A-CA64-437B-B415-3BD03004A544")]
pub unsafe trait _ISKQuoteLibEvents: IDispatch {}
// QuoteLib GUID
#[interface("9AF518AE-363A-4555-81F2-37D39CD6B7D9")]
pub unsafe trait ISKQuoteLib: IDispatch {}

#[interface("8DB2F1B3-20D6-4EC3-9614-CC6154B6FF72")]
pub unsafe trait _ISKOSQuoteLibEvents: IDispatch {}


#[implement(_ISKQuoteLibEvents)]
pub struct DomesticSink{
    handler:Box<dyn DomesticEvents + Send + Sync>,
}
#[implement(_ISKOSQuoteLibEvents)]
pub struct OverseaSink{
    handler:Box<dyn OverseaEvents + Send + Sync>,
}

impl DomesticSink {
    pub fn new<T: DomesticEvents + Send + Sync + 'static>(handler: T) -> Self {
        Self { handler: Box::new(handler) }
    }

    /// -- helpers: 從 VARIANT 取值 --
    /// 注意：COM 端宣告幾乎全是 c_short / c_int / BSTR
    unsafe fn v_i32(v: &VARIANT) -> i32 {
        // VT_I4 / VT_INT 常見；若來源給 VT_I2 你可再轉型
        match v.Anonymous.Anonymous.vt {
            VT_I4 | VT_INT => v.Anonymous.Anonymous.Anonymous.lVal,
            VT_I2 => v.Anonymous.Anonymous.Anonymous.iVal as i32,
            _ => v.Anonymous.Anonymous.Anonymous.lVal,
        }
    }

    unsafe fn v_i16(v: &VARIANT) -> i16 {
        match v.Anonymous.Anonymous.vt {
            VT_I2 => v.Anonymous.Anonymous.Anonymous.iVal,
            VT_I4 | VT_INT => v.Anonymous.Anonymous.Anonymous.lVal as i16,
            _ => v.Anonymous.Anonymous.Anonymous.iVal,
        }
    }

    unsafe fn v_bstr(v: &VARIANT) -> String {
        if v.Anonymous.Anonymous.vt == VT_BSTR {
            (&*v.Anonymous.Anonymous.Anonymous.bstrVal).to_string()
        } else {
            String::new()
        }
    }
    // / 取出 `DISPPARAMS` 的 args（**反序**）
    // unsafe fn args<'a>(dp: *const DISPPARAMS) -> &'a [VARIANT] {
    //     let dp = &*dp;
    //     slice::from_raw_parts(dp.rgvarg, dp.cArgs as usize)
    // }
}
impl IDispatch_Impl for DomesticSink_Impl {
    fn GetTypeInfoCount(&self) -> Result<u32> { Ok(0) }
    fn GetTypeInfo(&self, _i: u32, _lcid: u32) -> Result<ITypeInfo> { Err(E_NOTIMPL.into()) }
    fn GetIDsOfNames(&self, _riid: *const windows_core::GUID, _rgsznames: *const windows_core::PCWSTR, _cnames: u32, _lcid: u32, _rgdispid: *mut i32) -> windows_core::Result<()> {
        Err(E_NOTIMPL.into())
    }

    fn Invoke(
        &self,
        dispidmember: i32,
        _riid: *const GUID,
        _lcid: u32,
        _wflags: DISPATCH_FLAGS,
        pdispparams: *const DISPPARAMS,
        _pvarresult: *mut VARIANT,
        _pexcepinfo: *mut EXCEPINFO,
        _puargerr: *mut u32,
    ) -> Result<()> {
        unsafe {
            println!("invoke with id: {:?}", dispidmember);
            let params = *pdispparams;
            let args = std::slice::from_raw_parts(params.rgvarg, params.cArgs as usize);
            let mut reversed_args: Vec<_> = args.iter().cloned().collect();
            reversed_args.reverse();

            let hr=dispatch_domesticevents(&*self.handler, dispidmember, &reversed_args);
            hr.ok()
        }
    }
}
impl _ISKQuoteLibEvents_Impl for DomesticSink_Impl{}


impl OverseaSink{
    pub fn new<T: OverseaEvents + Send + Sync + 'static>(handler: T) -> Self {
        Self { handler: Box::new(handler) }
    }
}
impl IDispatch_Impl for OverseaSink_Impl{
    fn GetTypeInfoCount(&self) -> Result<u32> { Ok(0) }
    fn GetTypeInfo(&self, _i: u32, _lcid: u32) -> Result<ITypeInfo> { Err(E_NOTIMPL.into()) }
    fn GetIDsOfNames(&self, _riid: *const windows_core::GUID, _rgsznames: *const windows_core::PCWSTR, _cnames: u32, _lcid: u32, _rgdispid: *mut i32) -> windows_core::Result<()> {
        Err(E_NOTIMPL.into())
    }

    fn Invoke(
        &self,
        dispidmember: i32,
        _riid: *const GUID,
        _lcid: u32,
        _wflags: DISPATCH_FLAGS,
        pdispparams: *const DISPPARAMS,
        _pvarresult: *mut VARIANT,
        _pexcepinfo: *mut EXCEPINFO,
        _puargerr: *mut u32,
    ) -> Result<()> {
        unsafe {
            println!("invoke with id: {:?}", dispidmember);
            let params = *pdispparams;
            let args = std::slice::from_raw_parts(params.rgvarg, params.cArgs as usize);
            let mut reversed_args: Vec<_> = args.iter().cloned().collect();
            reversed_args.reverse();

            let hr=dispatch_overseaevents(&*self.handler, dispidmember, &reversed_args);
            hr.ok()
        }
    }
}
impl _ISKOSQuoteLibEvents_Impl for OverseaSink_Impl {}

