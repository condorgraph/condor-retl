// @ts-check

const lightCodeTheme = require("prism-react-renderer").themes.github;
const darkCodeTheme = require("prism-react-renderer").themes.dracula;

const googleAnalyticsMeasurementId = "G-ZKV41JJNZT";

/** @type {import("@docusaurus/types").Config} */
const config = {
  title: "Condor RETL Docs",
  tagline: "Task-oriented documentation for building and operating RETL syncs.",
  favicon: "img/favicon.ico",

  url: "https://docs.condorgraph.com",
  baseUrl: "/",

  organizationName: "Condor",
  projectName: "condor-retl",

  onBrokenLinks: "throw",
  onBrokenAnchors: "throw",
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "throw",
      onBrokenMarkdownImages: "throw",
    },
  },
  trailingSlash: true,
  headTags: [
    {
      tagName: "link",
      attributes: {
        rel: "preconnect",
        href: "https://api.fontshare.com",
      },
    },
    {
      tagName: "link",
      attributes: {
        rel: "stylesheet",
        href: "https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700,900&display=swap",
      },
    },
  ],

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      /** @type {import("@docusaurus/preset-classic").Options} */
      ({
        docs: {
          routeBasePath: "/",
          sidebarPath: require.resolve("./sidebars.js"),
          showLastUpdateAuthor: false,
          showLastUpdateTime: false,
        },
        blog: false,
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
        gtag: {
          trackingID: googleAnalyticsMeasurementId,
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import("@docusaurus/preset-classic").ThemeConfig} */
    ({
      navbar: {
        title: "CONDOR",
        logo: {
          alt: "Condor",
          src: "img/condor-logo-square.png",
        },
        items: [
          {
            type: "docSidebar",
            sidebarId: "userDocs",
            position: "left",
            label: "Docs",
          },
          {
            to: "/quickstart",
            label: "Install skills",
            position: "left",
          },
          {
            to: "/examples/duckdb-to-meta",
            label: "Examples",
            position: "left",
          },
          {
            href: "https://condorgraph.com/",
            label: "Main site",
            position: "right",
          },
        ],
      },
      footer: {
        style: "dark",
        logo: {
          alt: "Condor",
          src: "img/condor-logo-square.png",
          width: 32,
          height: 32,
        },
        links: [
          {
            title: "Use RETL",
            items: [
              {
                label: "Install skills",
                to: "/quickstart",
              },
              {
                label: "Install",
                to: "/guides/install",
              },
              {
                label: "Connector packages",
                to: "/reference/connector-packages",
              },
            ],
          },
          {
            title: "Operate",
            items: [
              {
                label: "Dry runs and recovery",
                to: "/guides/dry-runs-and-recovery",
              },
              {
                label: "Runtime commands",
                to: "/reference/runtime-commands",
              },
            ],
          },
          {
            title: "Condor",
            items: [
              {
                label: "Main site",
                href: "https://condorgraph.com/",
              },
              {
                label: "Condor Graph",
                href: "https://condorgraph.com/graph",
              },
              {
                label: "Activation",
                href: "https://condorgraph.com/activation",
              },
            ],
          },
        ],
        copyright: "© 2026 Condor",
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      colorMode: {
        defaultMode: "dark",
        respectPrefersColorScheme: false,
      },
    }),
};

module.exports = config;
