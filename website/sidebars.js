/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Getting started',
      collapsed: false,
      items: [
        'getting-started/quick-start',
        'getting-started/discord-setup',
        'getting-started/first-run',
        'getting-started/configuration',
        'getting-started/manual-install',
        'getting-started/docker-install',
        'getting-started/prerequisites',
        'getting-started/upgrading-from-v3',
      ],
    },
    {
      type: 'category',
      label: 'Web dashboard',
      items: [
        'dashboard/overview',
        'dashboard/guild-mappings',
        'dashboard/guilds',
        'dashboard/channels',
        'dashboard/messages',
        'dashboard/filters',
        {
          type: 'category',
          label: 'Tasks',
          link: {type: 'doc', id: 'dashboard/tasks'},
          items: [
            'dashboard/backfill',
            'dashboard/purge-and-sync',
            'dashboard/exports',
            'dashboard/scraper',
          ],
        },
        'dashboard/forwarding',
        'dashboard/backups',
        'dashboard/system',
        'dashboard/logs',
      ],
    },
    {
      type: 'category',
      label: 'Configuration',
      items: [
        'configuration/security',
        'configuration/cloning-options',
        'configuration/advanced',
      ],
    },
    {
      type: 'category',
      label: 'Slash commands',
      items: [
        'commands/overview',
        'commands/monitoring',
        'commands/filtering',
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
        'cli',
        'contributing',
      ],
    },
  ],
};

module.exports = sidebars;
