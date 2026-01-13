# Hotel-Booking-Big-Data-Pipeline
# Real-Time Big Data Pipeline: Hotel Booking Analytics

Αυτό το project υλοποιεί μια ολοκληρωμένη ροή δεδομένων μεγάλου όγκου (end-to-end big data pipeline) για την ανάλυση συμπεριφοράς χρηστών σε μια πλατφόρμα κρατήσεων ξενοδοχείων σε πραγματικό χρόνο.

## Αρχιτεκτονική Συστήματος
Το σύστημα αποτελείται από τέσσερα κύρια στάδια:

1.  **Data Simulation:** Παραγωγή ρεαλιστικών clickstream δεδομένων (χρήστες, συνεδρίες, αναζητήσεις, κρατήσεις) σε Python.
2.  **Data Ingestion (Kafka):** Ένας Kafka Producer στέλνει τα δεδομένα σε πραγματικό χρόνο σε έναν απομακρυσμένο Broker.
3.  **Stream Processing (Spark):** Χρήση Spark Structured Streaming (μέσω Databricks) για κανονικοποίηση, φιλτράρισμα και χρονική ομαδοποίηση (windowed aggregation) των δεδομένων ανά 50 λεπτά.
4.  **Storage & Analytics (MongoDB):** Αποθήκευση ωμών (raw) και επεξεργασμένων (aggregated) δεδομένων σε NoSQL βάση MongoDB Atlas.

## Τεχνολογίες που χρησιμοποιήθηκαν
* **Γλώσσα:** Python 3.x
* **Streaming:** Apache Kafka
* **Big Data Framework:** Apache Spark (PySpark)
* **Πλατφόρμα:** Databricks (Runtime 12.2 LTS)
* **Βάση Δεδομένων:** MongoDB Atlas (NoSQL)

## Περιγραφή Αρχείων
* [cite_start]`hotel_sim.py`: Εξομοιωτής που παράγει το αρχείο `hotel_clickstream.csv` με 1000 χρήστες. [cite: 37, 41]
* [cite_start]`kafka_producer.py`: Διαβάζει το CSV και στέλνει τα events στον Kafka broker με προσαρμοσμένο timestamp ("live" προσομοίωση). [cite: 62, 74]
* `spark.py`: Η "καρδιά" του project. [cite_start]Διαβάζει από τον Kafka, εφαρμόζει watermarks 30 λεπτών και υπολογίζει στατιστικά ανά πόλη. [cite: 148, 162, 168]
* [cite_start]`mongoQueries.py`: Script για την εξαγωγή επιχειρηματικών συμπερασμάτων από τη MongoDB (π.χ. πόλη με τις περισσότερες κρατήσεις). [cite: 205, 213]

## Βασικά Αποτελέσματα
Από την ανάλυση των δεδομένων προέκυψαν insights όπως:
* [cite_start]**Top Destination:** Το Ντουμπάι είχε τις περισσότερες αναζητήσεις και κρατήσεις. [cite: 229]
* [cite_start]**Stay Duration:** Η Νέα Υόρκη παρουσίασε τη μεγαλύτερη μέση διάρκεια παραμονής (9.67 ημέρες). [cite: 230]

---
*Αυτή η εργασία υλοποιήθηκε στο πλαίσιο του μαθήματος "Συστήματα Διαχείρισης Μεγάλων Δεδομένων" (2024-2025) και πήρε βαθμό 10/10.*
