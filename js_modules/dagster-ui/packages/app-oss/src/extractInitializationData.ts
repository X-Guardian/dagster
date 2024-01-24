const ELEMENT_ID = 'initialization-data';
const PREFIX_PLACEHOLDER = '__PATH_PREFIX__';
const TELEMETRY_PLACEHOLDER = '__TELEMETRY_ENABLED__';
const LIVE_DATA_POLL_RATE = '__LIVE_DATA_POLL_RATE__';
const HAS_CODE_LINKS_PLACEHOLDER = '__CODE_LINKS_ENABLED__';

let value:
  | {
      pathPrefix: string;
      telemetryEnabled: boolean;
      liveDataPollRate?: number;
      codeLinksEnabled: boolean;
    }
  | undefined = undefined;

// Determine the path prefix value, which is set server-side.
// This value will be used for prefixing paths for the GraphQL
// endpoint and dynamically loaded bundles.
export const extractInitializationData = (): {
  pathPrefix: string;
  telemetryEnabled: boolean;
  liveDataPollRate?: number;
  codeLinksEnabled: boolean;
} => {
  if (!value) {
    value = {pathPrefix: '', telemetryEnabled: false, codeLinksEnabled: false};
    const element = document.getElementById(ELEMENT_ID);
    if (element) {
      const parsed = JSON.parse(element.innerHTML);
      if (parsed.pathPrefix !== PREFIX_PLACEHOLDER) {
        value.pathPrefix = parsed.pathPrefix;
      }
      if (parsed.telemetryEnabled !== TELEMETRY_PLACEHOLDER) {
        value.telemetryEnabled = parsed.telemetryEnabled;
      }
      if (parsed.liveDataPollRate !== LIVE_DATA_POLL_RATE) {
        value.liveDataPollRate = parsed.liveDataPollRate;
      }
      if (parsed.codeLinksEnabled !== HAS_CODE_LINKS_PLACEHOLDER) {
        value.codeLinksEnabled = parsed.codeLinksEnabled;
      }
    }
  }
  return value;
};
