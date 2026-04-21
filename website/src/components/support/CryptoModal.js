import React, { useState } from 'react';
import { cryptoCurrencies } from '../../config/donationConfig';
import { useClipboard } from '../../hooks/useClipboard';
import styles from './support.module.css';

export default function CryptoModal({ isOpen, onClose }) {
  const [selectedCrypto, setSelectedCrypto] = useState(null);
  const { copied, copy } = useClipboard();

  if (!isOpen) return null;

  const handleBack = () => setSelectedCrypto(null);

  const handleClose = () => {
    setSelectedCrypto(null);
    onClose();
  };

  const handleOverlayClick = (e) => {
    if (e.target === e.currentTarget) handleClose();
  };

  return (
    <div className={styles.modalOverlay} onClick={handleOverlayClick}>
      <div className={styles.modal}>
        <button className={styles.modalClose} onClick={handleClose}>
          &times;
        </button>

        {!selectedCrypto ? (
          <>
            <h2 className={styles.modalTitle}>Choose Cryptocurrency</h2>
            <p className={styles.modalSubtitle}>
              Select a cryptocurrency to view the wallet address.
            </p>
            <div className={styles.cryptoGrid}>
              {cryptoCurrencies.map((crypto) => (
                <button
                  key={crypto.symbol}
                  className={styles.cryptoOption}
                  style={{ '--crypto-color': crypto.color }}
                  onClick={() => setSelectedCrypto(crypto)}
                >
                  <span className={styles.cryptoIcon}>{crypto.icon}</span>
                  <span className={styles.cryptoName}>{crypto.name}</span>
                  <span className={styles.cryptoSymbol}>{crypto.symbol}</span>
                </button>
              ))}
            </div>
          </>
        ) : (
          <>
            <button className={styles.backButton} onClick={handleBack}>
              &larr; Back
            </button>
            <h2 className={styles.modalTitle}>
              <span
                className={styles.cryptoIcon}
                style={{ color: selectedCrypto.color }}
              >
                {selectedCrypto.icon}
              </span>{' '}
              {selectedCrypto.name} ({selectedCrypto.symbol})
            </h2>
            <p className={styles.modalSubtitle}>
              Send {selectedCrypto.symbol} to the following address:
            </p>
            <div className={styles.addressContainer}>
              <code className={styles.walletAddress}>
                {selectedCrypto.address}
              </code>
              <button
                className={`${styles.copyButton} ${copied ? styles.copied : ''}`}
                onClick={() => copy(selectedCrypto.address)}
              >
                {copied ? 'Copied!' : 'Copy'}
              </button>
            </div>
            <div className={styles.qrContainer}>
              <img
                src={`https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(selectedCrypto.address)}`}
                alt={`${selectedCrypto.name} QR Code`}
                className={styles.qrCode}
                width={200}
                height={200}
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}
