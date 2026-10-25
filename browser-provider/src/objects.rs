//! Browser Objects — SpaceOS object representations for browser resources.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use space_core::Object;
use uuid::Uuid;

/// Object type for browser tabs.
pub const BROWSER_TAB_TYPE: &str = "BrowserTab";
/// Object type for browser pages.
pub const BROWSER_PAGE_TYPE: &str = "BrowserPage";

/// Create a BrowserTab object from CDP target info.
pub fn tab_object_type() -> &'static str {
    BROWSER_TAB_TYPE
}

/// Create a BrowserPage object type.
pub fn page_object_type() -> &'static str {
    BROWSER_PAGE_TYPE
}

/// BrowserTab object representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserTabObject {
    pub id: String,
    pub target_id: String,
    pub url: String,
    pub title: Option<String>,
    pub provider_id: String,
    pub external_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl BrowserTabObject {
    pub fn from_canonical(object: &Object)->Result<Self,serde_json::Error> {
        let p=&object.properties;
        Ok(Self{id:object.id.clone(),target_id:serde_json::from_value(p["target_id"].clone())?,url:serde_json::from_value(p["url"].clone())?,title:serde_json::from_value(p["title"].clone())?,
            provider_id:object.provider_id.clone(),external_id:object.external_id.clone(),created_at:object.created_at,updated_at:object.updated_at})
    }
    pub fn new(provider_id: &str, target_id: &str, url: &str, title: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            id: format!("browser-tab-{}", Uuid::new_v4()),
            target_id: target_id.to_string(),
            url: url.to_string(),
            title,
            provider_id: provider_id.to_string(),
            external_id: Some(target_id.to_string()),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn to_space_object(&self) -> Object {
        let mut obj = Object::new(
            BROWSER_TAB_TYPE,
            &self.provider_id,
            json!({
                "target_id": self.target_id,
                "url": self.url,
                "title": self.title,
            }),
        );
        obj.id = self.id.clone();
        obj.external_id = self.external_id.clone();
        obj.created_at = self.created_at;
        obj.updated_at = self.updated_at;
        obj
    }
}

/// BrowserPage object representation (deeper page state).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserPageObject {
    pub id: String,
    pub target_id: String,
    pub url: String,
    pub title: Option<String>,
    pub html: Option<String>,
    pub provider_id: String,
    pub external_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl BrowserPageObject {
    pub fn new(
        provider_id: &str,
        target_id: &str,
        url: &str,
        title: Option<String>,
        html: Option<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: format!("browser-page-{}", Uuid::new_v4()),
            target_id: target_id.to_string(),
            url: url.to_string(),
            title,
            html,
            provider_id: provider_id.to_string(),
            external_id: Some(target_id.to_string()),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn to_space_object(&self) -> Object {
        let mut props = json!({
            "target_id": self.target_id,
            "url": self.url,
            "title": self.title,
        });
        if let Some(html) = &self.html {
            props["html"] = json!(html);
        }

        let mut obj = Object::new(BROWSER_PAGE_TYPE, &self.provider_id, props);
        obj.id = self.id.clone();
        obj.external_id = self.external_id.clone();
        obj.created_at = self.created_at;
        obj.updated_at = self.updated_at;
        obj
    }
}

/// Event types emitted by the browser provider.
pub mod events {
    pub const BROWSER_TAB_CREATED: &str = "BrowserTabCreated";
    pub const BROWSER_TAB_UPDATED: &str = "BrowserTabUpdated";
    pub const BROWSER_TAB_CLOSED: &str = "BrowserTabClosed";
    pub const BROWSER_NAVIGATED: &str = "BrowserNavigated";
    pub const BROWSER_LOADED: &str = "BrowserLoaded";
    pub const BROWSER_ACTION_EXECUTED: &str = "BrowserActionExecuted";
}

/// Browser action types.
pub mod actions {
    pub const NAVIGATE: &str = "navigate";
    pub const RELOAD: &str = "reload";
    pub const GET_TABS: &str = "get_tabs";
    pub const GET_PAGE_CONTENT: &str = "get_page_content";
    /// Live frame of a tab (CDP `Page.captureScreenshot`), encoded
    /// base64. Read-only visual state for the SpaceOS World browser
    /// panel; never interacts with the page.
    pub const SCREENSHOT: &str = "screenshot";
    pub const EXECUTE_SCRIPT: &str = "execute_script";
    pub const NEW_TAB: &str = "new_tab";
    pub const ACTIVATE_TAB: &str = "activate_tab";
    pub const CLOSE_TAB: &str = "close_tab";
}
