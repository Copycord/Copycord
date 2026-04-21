import React from 'react';
import SupportHero from './SupportHero';
import DonationMethods from './DonationMethods';
import AlternativeSupport from './AlternativeSupport';
import styles from './support.module.css';

export default function SupportPage() {
  return (
    <div className={styles.supportPage}>
      <SupportHero />
      <div className={styles.contentContainer}>
        <DonationMethods />
        <AlternativeSupport />
      </div>
    </div>
  );
}
