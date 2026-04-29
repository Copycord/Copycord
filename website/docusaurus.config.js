// @ts-check

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Copycord',
  tagline: 'The ultimate Discord server mirroring tool',
  favicon: 'img/logo.png',

  url: 'https://copycord.github.io',
  baseUrl: '/Copycord/',

  organizationName: 'Copycord',
  projectName: 'Copycord',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      /** @type {import("@easyops-cn/docusaurus-search-local").PluginOptions} */
      ({
        hashed: true,
        language: ['en'],
        indexBlog: false,
        docsRouteBasePath: '/docs',
      }),
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/Copycord/Copycord/tree/main/website/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/logo.png',
      colorMode: {
        defaultMode: 'dark',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Copycord',
        logo: {
          alt: 'Copycord Logo',
          src: 'img/logo.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docs',
            position: 'left',
            label: 'Docs',
          },
          {
            to: '/support',
            label: 'Support',
            position: 'left',
          },
          {
            href: 'https://github.com/Copycord/Copycord',
            label: 'GitHub',
            position: 'right',
          },
          {
            href: 'https://discord.gg/ArFdqrJHBj',
            label: 'Discord',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              { label: 'Getting Started', to: '/docs/getting-started/prerequisites' },
              { label: 'Dashboard', to: '/docs/dashboard/overview' },
              { label: 'Commands', to: '/docs/commands/overview' },
              { label: 'Configuration', to: '/docs/configuration/cloning-options' },
            ],
          },
          {
            title: 'Community',
            items: [
              { label: 'Discord', href: 'https://discord.gg/ArFdqrJHBj' },
              { label: 'GitHub Issues', href: 'https://github.com/Copycord/Copycord/issues' },
            ],
          },
          {
            title: 'More',
            items: [
              { label: 'GitHub', href: 'https://github.com/Copycord/Copycord' },
              { label: 'Releases', href: 'https://github.com/Copycord/Copycord/releases' },
              { label: 'Support Us', to: '/support' },
            ],
          },
        ],
        copyright: `Copyright ${new Date().getFullYear()} Copycord. Built with Docusaurus.`,
      },
      prism: {
        theme: require('prism-react-renderer').themes.github,
        darkTheme: require('prism-react-renderer').themes.dracula,
        additionalLanguages: ['bash', 'yaml', 'json', 'python'],
      },
    }),
};

module.exports = config;
