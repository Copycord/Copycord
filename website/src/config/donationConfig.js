export const donationMethods = [
  {
    id: 'kofi',
    title: 'Ko-fi',
    description: 'Support Copycord with a one-time or recurring donation through Ko-fi.',
    icon: 'coffee',
    url: 'https://ko-fi.com/A0A41KPDX4',
    buttonText: 'Buy Me a Coffee',
    accentColor: '#ff5e5b',
    featured: true,
    featuredLabel: 'Recommended',
  },
  {
    id: 'crypto',
    title: 'Cryptocurrency',
    description: 'Donate using your preferred cryptocurrency. Click to view wallet addresses.',
    icon: 'crypto',
    buttonText: 'View Addresses',
    accentColor: '#f7931a',
    featured: false,
    isModal: true,
  },
];

export const cryptoCurrencies = [
  {
    name: 'Bitcoin',
    symbol: 'BTC',
    address: '1FMoe24HeA9DAiAjDKrDVe7FxBbmAkx9Fi',
    color: '#f7931a',
    icon: '₿',
  },
  {
    name: 'Ethereum',
    symbol: 'ETH',
    address: '0xac43d0adb7e2d87ab887a5117f5e2254da5e7c3e',
    color: '#627eea',
    icon: 'Ξ',
  },
];

export const alternativeSupport = [
  {
    title: 'Star on GitHub',
    description: 'Show your support by starring the repository.',
    icon: '⭐',
    url: 'https://github.com/Copycord/Copycord',
  },
  {
    title: 'Report Bugs',
    description: 'Help us improve by reporting issues you find.',
    icon: '🐛',
    url: 'https://github.com/Copycord/Copycord/issues',
  },
  {
    title: 'Join Discord',
    description: 'Connect with the community and get support.',
    icon: '💬',
    url: 'https://discord.gg/ArFdqrJHBj',
  },
  {
    title: 'Share the Project',
    description: 'Tell others about Copycord and help us grow.',
    icon: '📢',
  },
  {
    title: 'Contribute Code',
    description: 'Submit pull requests to add features or fix bugs.',
    icon: '💻',
    url: 'https://github.com/Copycord/Copycord/pulls',
  },
  {
    title: 'Write Documentation',
    description: 'Help improve the docs for new and existing users.',
    icon: '📝',
    url: 'https://github.com/Copycord/Copycord/tree/main/website',
  },
];
