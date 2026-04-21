/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'getting-started/prerequisites',
        'getting-started/discord-setup',
        'getting-started/docker-install',
        'getting-started/manual-install',
        'getting-started/first-run',
      ],
    },
    {
      type: 'category',
      label: 'Web Dashboard',
      items: [
        'dashboard/overview',
        'dashboard/guild-mappings',
        'dashboard/channels',
        'dashboard/filters',
        'dashboard/backfill',
        'dashboard/forwarding',
        'dashboard/scraper',
        'dashboard/backups',
        'dashboard/system',
      ],
    },
    {
      type: 'category',
      label: 'Configuration',
      items: [
        'configuration/cloning-options',
        'configuration/advanced',
      ],
    },
    {
      type: 'category',
      label: 'Slash Commands',
      items: [
        'commands/overview',
        'commands/monitoring',
        'commands/filtering',
        'commands/announcements',
        'commands/roles',
        'commands/assets',
        'commands/webhooks',
        'commands/exports',
        'commands/rewrites',
      ],
    },
    {
      type: 'category',
      label: 'Features',
      items: [
        'features/message-cloning',
        'features/structure-sync',
        'features/message-forwarding',
        'features/member-scraping',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'contributing',
      ],
    },
  ],
};

module.exports = sidebars;
