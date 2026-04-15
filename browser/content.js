// FIS BIL Content Script — captures scroll depth and copy events

let scrolledToBottom = false;
let copiedText = false;

// Track scroll depth
window.addEventListener("scroll", () => {
  const scrollHeight = document.documentElement.scrollHeight;
  const scrollTop = window.scrollY;
  const clientHeight = window.innerHeight;

  if (scrollTop + clientHeight >= scrollHeight * 0.9) {
    scrolledToBottom = true;
  }
});

// Track copy events
document.addEventListener("copy", () => {
  copiedText = true;
});

// Send data to background before page unload
window.addEventListener("beforeunload", () => {
  chrome.runtime.sendMessage({
    type: "page_data",
    scrolledBottom: scrolledToBottom,
    copied: copiedText,
    url: window.location.href,
    wordCount: document.body?.innerText?.split(/\s+/).length || 0,
  });
});
