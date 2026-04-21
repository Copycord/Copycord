import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import styles from './index.module.css';

const features = [
  {
    title: 'Multi-Server Cloning',
    icon: '🔄',
    description:
      'Mirror entire Discord servers including channels, roles, emojis, stickers, and message history — all in real time.',
  },
  {
    title: 'Live Message Sync',
    icon: '⚡',
    description:
      'Every message, edit, and delete is forwarded instantly via webhooks, keeping your clone perfectly in sync.',
  },
  {
    title: 'Web Dashboard',
    icon: '🖥️',
    description:
      'Manage everything through a sleek, modern web interface. Configure mappings, filters, backfills, and more.',
  },
  {
    title: 'Message Forwarding',
    icon: '📤',
    description:
      'Forward messages to Telegram, Pushover, or any webhook with flexible keyword and channel filters.',
  },
  {
    title: 'Deep History Import',
    icon: '📚',
    description:
      "Backfill entire channel histories, not just new messages. Import months or years of messages.",
  },
  {
    title: 'Easy Setup',
    icon: '🐳',
    description:
      'Get started in minutes with Docker or the manual installer. Works on Windows, Linux, and macOS.',
  },
];

function Feature({ title, icon, description }) {
  return (
    <div className={clsx('col col--4')}>
      <div className="feature-card" style={{ marginBottom: '1.5rem' }}>
        <div className="feature-icon">{icon}</div>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <div className="container">
        <img
          src="img/logo.png"
          alt="Copycord Logo"
          style={{ width: 120, marginBottom: '1rem' }}
        />
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/getting-started/prerequisites"
          >
            Get Started
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro"
            style={{ marginLeft: '1rem' }}
          >
            Read the Docs
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout title="Home" description={siteConfig.tagline}>
      <HomepageHeader />
      <main>
        <section className="features">
          <div className="container">
            <div className="row">
              {features.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
