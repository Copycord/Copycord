import React from 'react';
import styles from './support.module.css';

function CardIcon({ icon }) {
  switch (icon) {
    case 'coffee':
      return (
        <svg viewBox="0 0 24 24" width="28" height="28" fill="currentColor">
          <path d="M2 21h18v-2H2v2zM20 8h-2V5H4v3H2v2c0 2.76 2.24 5 5 5h4c2.76 0 5-2.24 5-5h2c1.1 0 2-.9 2-2V8zm-4 2c0 1.66-1.34 3-3 3H7c-1.66 0-3-1.34-3-3V7h12v3zm4-2h-2V8h2v0z" />
        </svg>
      );
    case 'crypto':
      return (
        <svg viewBox="0 0 24 24" width="28" height="28" fill="currentColor">
          <path d="M11.5 11.5v-2h1.25c.55 0 1 .45 1 1s-.45 1-1 1H11.5zm0 1.5h1.25c.55 0 1 .45 1 1s-.45 1-1 1H11.5V13zm3.5 1c0 .83-.42 1.56-1.06 2H15v1.5h-1.5v1H13v-1h-2v1H9.5v-1H8V16h1.06C8.42 15.56 8 14.83 8 14c0-.57.2-1.1.53-1.5C8.2 12.1 8 11.57 8 11c0-.83.42-1.56 1.06-2H8V7.5h1.5v-1H11v1h2v-1h1.5v1H16V9h-1.06c.64.44 1.06 1.17 1.06 2 0 .57-.2 1.1-.53 1.5.33.4.53.93.53 1.5zM12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z" />
        </svg>
      );
    default:
      return null;
  }
}

export default function DonationCard({ method, onModalOpen }) {
  const handleClick = () => {
    if (method.isModal && onModalOpen) {
      onModalOpen();
    } else if (method.url) {
      window.open(method.url, '_blank', 'noopener,noreferrer');
    }
  };

  return (
    <div
      className={`${styles.donationCard} ${method.featured ? styles.featured : ''}`}
      style={{ '--card-accent-color': method.accentColor }}
    >
      {method.featured && (
        <span className={styles.featuredBadge}>{method.featuredLabel}</span>
      )}
      <div className={styles.cardIcon}>
        <CardIcon icon={method.icon} />
      </div>
      <h3 className={styles.cardTitle}>{method.title}</h3>
      <p className={styles.cardDescription}>{method.description}</p>
      <button
        className={`${styles.cardButton} ${styles[`${method.id}Btn`]}`}
        onClick={handleClick}
      >
        {method.buttonText}
      </button>
    </div>
  );
}
