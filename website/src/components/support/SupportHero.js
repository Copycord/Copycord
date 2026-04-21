import React from 'react';
import styles from './support.module.css';

export default function SupportHero() {
  return (
    <div className={styles.hero}>
      <div className={styles.heroContent}>
        <h1 className={styles.heroTitle}>Support Copycord</h1>
        <p className={styles.heroSubtitle}>
          Copycord is free and open-source. Your support helps keep development
          going, fund new features, and maintain the project for everyone.
        </p>
      </div>
    </div>
  );
}
