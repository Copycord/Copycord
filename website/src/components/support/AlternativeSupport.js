import React from 'react';
import { alternativeSupport } from '../../config/donationConfig';
import styles from './support.module.css';

export default function AlternativeSupport() {
  return (
    <section className={styles.alternativeSection}>
      <h2 className={styles.sectionTitle}>Other Ways to Help</h2>
      <div className={styles.alternativeGrid}>
        {alternativeSupport.map((item, index) => (
          <div key={index} className={styles.alternativeItem}>
            {item.url ? (
              <a
                href={item.url}
                target="_blank"
                rel="noopener noreferrer"
                className={styles.alternativeLink}
              >
                <span className={styles.alternativeIcon}>{item.icon}</span>
                <div>
                  <h3 className={styles.alternativeTitle}>{item.title}</h3>
                  <p className={styles.alternativeDescription}>
                    {item.description}
                  </p>
                </div>
              </a>
            ) : (
              <div className={styles.alternativeLink}>
                <span className={styles.alternativeIcon}>{item.icon}</span>
                <div>
                  <h3 className={styles.alternativeTitle}>{item.title}</h3>
                  <p className={styles.alternativeDescription}>
                    {item.description}
                  </p>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
      <div className={styles.thankYou}>
        <h3 className={styles.thankYouTitle}>Thank You!</h3>
        <p className={styles.thankYouText}>
          Every contribution, big or small, helps keep Copycord alive and
          growing. Whether you donate, report a bug, or share the project with a
          friend — you're making a difference.
        </p>
      </div>
    </section>
  );
}
