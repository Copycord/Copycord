import React from 'react';
import DonationCard from './DonationCard';
import CryptoModal from './CryptoModal';
import { donationMethods } from '../../config/donationConfig';
import { useModal } from '../../hooks/useModal';
import styles from './support.module.css';

export default function DonationMethods() {
  const cryptoModal = useModal();

  return (
    <section className={styles.donationSection}>
      <h2 className={styles.sectionTitle}>Donation Methods</h2>
      <div className={styles.donationGrid}>
        {donationMethods.map((method) => (
          <DonationCard
            key={method.id}
            method={method}
            onModalOpen={method.isModal ? cryptoModal.open : undefined}
          />
        ))}
      </div>
      <CryptoModal isOpen={cryptoModal.isOpen} onClose={cryptoModal.close} />
    </section>
  );
}
