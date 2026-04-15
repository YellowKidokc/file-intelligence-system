// FIS BIL Browser Extension — Background Service Worker
// Captures: page visits, time on page, bookmarks, tab closes

const BIL_ENDPOINT = "http://localhost:8420/bil/web";

// Track active tab times
const tabTimes = {};

// Tab activated — start timing
chrome.tabs.onActivated.addListener((activeInfo) => {
  const now = Date.now();
  // Stop timing previous tab
  for (const [tabId, data] of Object.entries(tabTimes)) {
    if (data.active) {
      data.active = false;
      data.totalTime += now - data.lastActivated;
    }
  }
  // Start timing new tab
  if (!tabTimes[activeInfo.tabId]) {
    tabTimes[activeInfo.tabId] = { totalTime: 0, lastActivated: now, active: true, url: "" };
  } else {
    tabTimes[activeInfo.tabId].lastActivated = now;
    tabTimes[activeInfo.tabId].active = true;
  }
});

// Tab updated — capture URL
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.url && tabTimes[tabId]) {
    tabTimes[tabId].url = changeInfo.url;
  }
});

// Tab closed — send behavioral data
chrome.tabs.onRemoved.addListener((tabId) => {
  const data = tabTimes[tabId];
  if (data) {
    const now = Date.now();
    if (data.active) {
      data.totalTime += now - data.lastActivated;
    }
    // Send to BIL
    sendToBIL({
      url: data.url,
      time_on_page: Math.round(data.totalTime / 1000),
      bookmarked: false,
      copied: false,
    });
    delete tabTimes[tabId];
  }
});

// Bookmark created — high signal
chrome.bookmarks.onCreated.addListener((id, bookmark) => {
  sendToBIL({
    url: bookmark.url,
    bookmarked: true,
    time_on_page: 0,
    copied: false,
  });
});

// Receive messages from content script (copy events, scroll depth)
chrome.runtime.onMessage.addListener((message, sender) => {
  if (message.type === "page_data") {
    const tabId = sender.tab?.id;
    if (tabId && tabTimes[tabId]) {
      // Enrich tab data
      tabTimes[tabId].scrolledBottom = message.scrolledBottom;
      tabTimes[tabId].copied = message.copied;
    }
  }
});

async function sendToBIL(data) {
  try {
    await fetch(BIL_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
  } catch (e) {
    // BIL server not running — silently ignore
  }
}
