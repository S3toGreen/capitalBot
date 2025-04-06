function waitForElm(selector) {
    return new Promise(resolve => {
        if (document.querySelector(selector)) {
            return resolve(document.querySelector(selector));
        }

        const observer = new MutationObserver(mutations => {
            if (document.querySelector(selector)) {
                observer.disconnect();
                resolve(document.querySelector(selector));
            }
        });

        // If you get "parameter 1 is not of type 'Node'" error, see https://stackoverflow.com/a/77855838/492336
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    });
};
var i;
function Loop(){
    setTimeout(async () => {
        i=document.querySelector("[aria-label=編輯草稿]");
        if (i==null)
            return;
        i.click()
        await waitForElm("#step-badge-3").then((ele)=>ele.click());
        await waitForElm("button[aria-label=儲存]").then((ele)=>ele.click());
        await waitForElm("#close-button [aria-label=關閉]").then((ele)=>ele.click());
        Loop();
    }, 1800);
}
for(var b; b=document.querySelector("[aria-label=編輯草稿]");b!=null){
document.querySelector("[aria-label=編輯草稿]").click();await waitForElm("#step-badge-3").then((ele)=>ele.click());await waitForElm("button[aria-label=儲存]").then((ele)=>ele.click());await waitForElm("#close-button [aria-label=關閉]").then((ele)=>ele.click())
}

t=[];document.querySelectorAll('#video-title').forEach((ele)=>t.push(parseInt(ele.title)));t.sort((a,b)=>a-b)
