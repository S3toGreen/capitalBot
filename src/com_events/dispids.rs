use windows::Win32::System::Variant::VARIANT;
use windows_core::HRESULT;
use windows::Win32::Foundation::DISP_E_MEMBERNOTFOUND;
macro_rules! define_event_handler {
    ($trait_name:ident, $enum_name:ident, { $($variant:ident = $id:expr),* $(,)? }) => {
        #[repr(i32)]
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        pub enum $enum_name {
            $( $variant = $id, )*
        }

        impl TryFrom<i32> for $enum_name {
            type Error = ();
            fn try_from(value: i32) -> Result<Self, Self::Error> {
                match value {
                    $( $id => Ok(Self::$variant), )*
                    _ => Err(()),
                }
            }
        }

        pub trait $trait_name {
            $( paste::paste! { fn [<on_ $variant:lower>] (&self, args: &[VARIANT]);} )*
        }

        paste::paste! { pub fn [<dispatch_ $trait_name:lower>] <T:?Sized + $trait_name>(
            handler: &T,
            dispid: i32,
            args: &[VARIANT],
        ) -> HRESULT {
            match $enum_name::try_from(dispid) {
                $( Ok($enum_name::$variant) => {
                    handler.[<on_ $variant:lower>] (args);
                    HRESULT(0)
                }, )*
                Err(_) => DISP_E_MEMBERNOTFOUND,
            }
        }}
    };
}

define_event_handler!(DomesticEvents, DomesticDispId, {
    Quote = 19,
    HistoryTick = 20,
    Ticks = 21,
    Depth = 22,
});

define_event_handler!(OverseaEvents, OverseaDispId, {
    Quote = 15,
    HistoryTick = 16,
    Ticks = 17,
    Depth = 19,
});

pub struct DMHandler;
impl DomesticEvents for DMHandler{
    fn on_depth(&self,args: &[VARIANT]) {
        
    }
    fn on_historytick(&self,args: &[VARIANT]) {
        
    }
    fn on_quote(&self,args: &[VARIANT]) {
        
    }
    fn on_ticks(&self,args: &[VARIANT]) {
        
    }
}

pub struct OSHandler;
impl OverseaEvents for OSHandler{
    fn on_depth(&self,args: &[VARIANT]) {
        
    }
    fn on_historytick(&self,args: &[VARIANT]) {
        
    }
    fn on_quote(&self,args: &[VARIANT]) {
        
    }
    fn on_ticks(&self,args: &[VARIANT]) {
        
    }
}

