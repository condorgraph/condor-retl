/** @type {import("@docusaurus/plugin-content-docs").SidebarsConfig} */
const sidebars = {
  userDocs: [
    "intro",
    "quickstart",
    {
      type: "category",
      label: "Concepts",
      collapsed: false,
      items: [
        "concepts/source-state-event-sync-destination",
        "concepts/runtime-phases",
      ],
    },
    {
      type: "category",
      label: "Guides",
      collapsed: false,
      items: [
        "guides/install",
        "guides/first-sync",
        "guides/configuration-and-secrets",
        "guides/dry-runs-and-recovery",
        "guides/destinations",
      ],
    },
    {
      type: "category",
      label: "Examples",
      collapsed: false,
      items: [
        "examples/duckdb-to-meta",
        "examples/event-sync",
        "examples/local-mock",
      ],
    },
    {
      type: "category",
      label: "Reference",
      collapsed: false,
      items: [
        "reference/runtime-commands",
        "reference/connector-packages",
        "reference/glossary",
      ],
    },
  ],
};

module.exports = sidebars;
