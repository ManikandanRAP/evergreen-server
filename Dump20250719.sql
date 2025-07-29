-- MySQL dump 10.13  Distrib 8.0.42, for Win64 (x86_64)
--
-- Host: localhost    Database: evergreen
-- ------------------------------------------------------
-- Server version	8.0.42

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `demographic`
--

DROP TABLE IF EXISTS `demographic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `demographic` (
  `show_id` char(36) DEFAULT NULL,
  `age_range` text,
  `gender` varchar(20) DEFAULT NULL,
  `region` enum('urban','rural','both') DEFAULT NULL,
  `primary_education` text,
  `secondary_education` text,
  KEY `show_id` (`show_id`),
  CONSTRAINT `demographic_ibfk_1` FOREIGN KEY (`show_id`) REFERENCES `shows` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `demographic`
--

LOCK TABLES `demographic` WRITE;
/*!40000 ALTER TABLE `demographic` DISABLE KEYS */;
INSERT INTO `demographic` VALUES ('cf5878b0-5b5e-4ce9-8378-7b197a0985c6',NULL,NULL,NULL,NULL,NULL),('18700f93-3e66-4b82-a5b3-ba367d847848','35-54','60/40','urban','High School','Posgraduate'),('3798563b-bb43-408b-9d9f-7268d9eeea53',NULL,NULL,NULL,NULL,NULL),('00863280-cd65-42f1-856b-462b5cca8700',NULL,NULL,NULL,NULL,NULL),('4f58dde7-b749-4341-a5b7-d548c3bd281a','35-54','70/30','urban','High School','Postgraduate'),('79f9b6db-fced-497d-92af-f30919b24feb','18-34','40/60','both','High School','College');
/*!40000 ALTER TABLE `demographic` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `genre`
--

DROP TABLE IF EXISTS `genre`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `genre` (
  `id` char(36) NOT NULL,
  `name` enum('History','Human Resources','Human Interest','Fun & Nostalgia','True Crime','Financial','News & Politics','Movies','Music','Religious','Health & Wellness','Parenting','Lifestyle','Storytelling','Literature','Sports','Pop Culture','Arts','Business','Philosophy') DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `genre`
--

LOCK TABLES `genre` WRITE;
/*!40000 ALTER TABLE `genre` DISABLE KEYS */;
INSERT INTO `genre` VALUES ('2b766031-3104-45e9-ab97-3a16cccd0b6a','Financial'),('45d5bfd2-2b57-455d-9f6c-8fca29092a0d','True Crime'),('edbb1d71-4504-4ee9-9c9e-d6c63a469978','News & Politics');
/*!40000 ALTER TABLE `genre` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ledger_transaction`
--

DROP TABLE IF EXISTS `ledger_transaction`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ledger_transaction` (
  `id` char(36) NOT NULL,
  `transaction_id` varchar(255) DEFAULT NULL,
  `show_id` char(36) DEFAULT NULL,
  `payment_date` date DEFAULT NULL,
  `amount_received` decimal(10,0) DEFAULT NULL,
  `customer_name` text,
  `advertiser_name` text,
  `description` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `transaction_id` (`transaction_id`),
  KEY `show_id` (`show_id`),
  CONSTRAINT `ledger_transaction_ibfk_1` FOREIGN KEY (`show_id`) REFERENCES `shows` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ledger_transaction`
--

LOCK TABLES `ledger_transaction` WRITE;
/*!40000 ALTER TABLE `ledger_transaction` DISABLE KEYS */;
/*!40000 ALTER TABLE `ledger_transaction` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `partners`
--

DROP TABLE IF EXISTS `partners`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `partners` (
  `id` char(36) NOT NULL,
  `user_id` char(36) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  CONSTRAINT `partners_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `partners`
--

LOCK TABLES `partners` WRITE;
/*!40000 ALTER TABLE `partners` DISABLE KEYS */;
/*!40000 ALTER TABLE `partners` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `revenue_split`
--

DROP TABLE IF EXISTS `revenue_split`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `revenue_split` (
  `id` char(36) NOT NULL,
  `advertiser_name` text,
  `split_type` enum('standard','programmatic') DEFAULT NULL,
  `partner_pct` decimal(10,0) DEFAULT NULL,
  `evergreen_pct` decimal(10,0) DEFAULT NULL,
  `effective_date` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `revenue_split`
--

LOCK TABLES `revenue_split` WRITE;
/*!40000 ALTER TABLE `revenue_split` DISABLE KEYS */;
/*!40000 ALTER TABLE `revenue_split` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `show_partners`
--

DROP TABLE IF EXISTS `show_partners`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `show_partners` (
  `id` char(36) NOT NULL,
  `show_id` char(36) DEFAULT NULL,
  `partner_id` char(36) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `show_id` (`show_id`),
  KEY `partner_id` (`partner_id`),
  CONSTRAINT `show_partners_ibfk_1` FOREIGN KEY (`show_id`) REFERENCES `shows` (`id`),
  CONSTRAINT `show_partners_ibfk_2` FOREIGN KEY (`partner_id`) REFERENCES `partners` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `show_partners`
--

LOCK TABLES `show_partners` WRITE;
/*!40000 ALTER TABLE `show_partners` DISABLE KEYS */;
/*!40000 ALTER TABLE `show_partners` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `shows`
--

DROP TABLE IF EXISTS `shows`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `shows` (
  `id` char(36) NOT NULL,
  `title` text,
  `minimum_guarantee` decimal(15,2) DEFAULT NULL,
  `annual_usd` json DEFAULT NULL,
  `subnetwork_id` char(36) DEFAULT NULL,
  `media_type` enum('video','audio','both') DEFAULT NULL,
  `tentpole` tinyint(1) NOT NULL DEFAULT '0',
  `relationship_level` enum('strong','medium','weak') DEFAULT NULL,
  `show_type` enum('Branded','Original','Partner') DEFAULT NULL,
  `evergreen_ownership_pct` decimal(5,3) DEFAULT NULL,
  `has_sponsorship_revenue` tinyint(1) DEFAULT NULL,
  `has_non_evergreen_revenue` tinyint(1) DEFAULT NULL,
  `requires_partner_access` tinyint(1) DEFAULT NULL,
  `has_branded_revenue` tinyint(1) DEFAULT NULL,
  `has_marketing_revenue` tinyint(1) DEFAULT NULL,
  `has_web_mgmt_revenue` tinyint(1) DEFAULT NULL,
  `genre_name` char(36) DEFAULT NULL,
  `is_original` tinyint(1) DEFAULT NULL,
  `shows_per_year` int DEFAULT NULL,
  `latest_cpm_usd` decimal(10,2) DEFAULT NULL,
  `ad_slots` int DEFAULT NULL,
  `avg_show_length_mins` int DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `show_name_in_qbo` enum('2020''d','36 From the Vault','3AM Scary Stories','A New Level','Accelerate Your Business Growth','ACME','Administrative','Age of Innocence','Ain''t It Scary','American Criminal','American Elections','American Vigilante','Anglo-Saxon England','Anthology of Heroes History','Around The World with Geoff','Authenica Misc','Axe to Grind','Banking Transformed','BEEF with Bridgett Todd','Beyond The OC','Branded Multi Show','Branded Shows','Break It Down with Matt Carter','Bridechilla','Broadway Nation','Broadway Podcast Network','Burn the Boats','Buy Hold Sell','Canned Air A Tribute to Pop Culture','Carol Costello Presents Blind Rage','Carol Costello Presents God Hook','Catch The Story!','Chris DeMakes a Podcast','Circles','Closereads','Cluster B Look At Narcissism Antisocial Borderline Disorders','Collect Call with Suge Knight','Comedy Store Studios','Conflicted A History Podcast','CONmunity Podcasts','Conning the Con','Consulting','Control Your Narrative','Countdown to Dallas','COVID19 Commonsense Conversations on the Pandemic','Crowd Network','Dakota Spotlight','Dan O Says So','DC EKG','Death of a Film Star','Death of a Rock Star','Death of a Sports Star','Delirious Nomads The Blacklight Media Podcast','Destination Cleveland','Diablo''s Den','Disinformation','Disorder','Disturbed','Doing It At Home','Don''t Retire...Graduate Podcast','dot com The Hacking','Double down with Breslo','Drinks With Johnny','Eliza A Robot Story','Evergreen NOW!','Fabulously Delicious','Fantasy Points Now','Feature Foundations','Five Minute News','Fly On The Call - Candid Conversations on Music','Focus to Evolve','From First Lady to Jackie O','Fruitloops Serial Killers of Color','Future Foundations','Future Friday','General Admin','GenXGrownUp Podcast','Get Tucked!','Gina Nicola','Guilty Greenie','Hammer Down Racing Report','Hardcore Surf History','Haven! Podcast','Having It A.L.L','HCM Tech Report','Healthy/Toxic Relationships w Narcissistic Borderline Other','Hear Her Sports','Hearing Jesus','Hearing Jesus for Kids','Heavy Spoilers','Helping Friendly Podcast','Her Half of History','Her Money','Here, Now, Together','High School Hamster Wheel','History Shorts','History Teachers Talking','Honest AF Show','How I Got Greenlit','Ideas Have Consequences','Impact of Influence','In The Cards','In the Key Of','Indecent with Kiki Anderson','Independent Podcast Network (IPN)','Informed Pregnancy','Inside Line F1','Inside the Boards','Inside the Musician''s Brain','Invisible Choir','Jim Cornette Drive Thru & Experience','Journey of Grace','Just Sayin'' with Justin Martindale','Kennedy Dynasty','Klooghless','Lay of the Land','Leadership Lean In','Litigate with Insight','Living for We','Made for This by Jennie Allen','Making Waves, The ShipRocked Podcast','Managerial Admin','Medal of Honor','Mental Health // Demystified','Millennial Santa','Minter Dialogue','Misc. Show Revenue','Mission Studios','MOD Network','Modes and Moods','Moods & Modes','Mr. Bunker''s Conspiracy Time Podcast','Murder in House Two','Nater on Wooden','New Mommy Media','Newbies','Next Best Picture Podcast','Next Chapter','Nintendo Dads Podcast','Nippon','No Revenue Shows','No Simple Road','Noble Rot','None But The Brave','Northern Disclosure','Novel Conversations','Ohio Jobs','Ohio Mysteries','Ohio V. The World','One Hit Thunder','One Two Me You','Original Shows','Original Shows - Multi Show','Orum','Osiris','Overnight Drive','Pantheon Podcasts','ParaReality','ParaTruth','Parent Savers','Partner Shows','Partner Shows - Multi Show','Pass The Mic','Philosophy vs Imrov','PIE','Pit Lane Parley','Pit Pass F1','Pit Pass Indy','Pit Pass Moto','Pit Pass Nascar','Play Me or Fade Me','Play On Podcasts','Podcast Your Business','Political Shadings','Pop Culture Confidential','Practical Stoicism','Prada Pod','Preggie Pals','Presidencies of the United States','Press Box Access A Sports History Podcast','Pretty Funny Business','Professional Book Nerds','Programmatic Only','PROHFILES | THE WRATH OF THE BUZZARD','PTO','Punk Stock','QEZ','Rabia and Ellyn Solve the Case','Ransom Notes','Recruiting Future','Restoring the Feminine','Return on Life Wealth Partners','Rhapsody Shows','Riffs on Riffs','Rocketship.fm','RON','Scams & Cons','Season Pass AF1','Self-Brain Surgery with Dr. Lee Warren','She Goes By Jane','Shenk','Shipwrecks and Seadogs','Shoveling Smoke','Sidetracked','Slaycation','Sleepover Cinema','Social Studies','Something About the Beatles','Somewhere On Earth The Global Tech Podcast','Sound Talent Media','Spit!','Spooky Science Sisters','StageCraft with Gordon Cox','Stagepass','Stateside Podcast','Stop the Killing','Storytime Anytime','Surf Splendor','Surf Stories','Swell with My Soul','That One Time On Tour','That''s Awesome With Joe','That''s Total Mom Sense','The 500 with Josh Adam Meyers','The Boardroom Podcast','The Boob Group','The Bouquet Toss','The Bravery Academy','The Brutally Delicious Podcast','The Chad and Cheese Podcast','The Chad Prather Show','The Cheese Wheel','The Clint Norris Show','The Corner of Grey Street','The Daily Music Business Podcast','The David Nurse Show','The Doctor & The Nurse','The Ex-Man with Doc Coyle','The F1 Strategy Report','The Freek Show Podcast with Twiztid','The French History Podcast','The Gratitude Podcast','The Grit!','The Health Beat','The Healthy Mama Kitchen Podcast','The Interning 101','The Jim Stroud Podcast','The Josh Potter Show','The Joy of Padel','The Kim Congdon Takeover','The Mad Scientist Podcast','The Malliard Report','The Manage Mental','The Metallica Report','The Musicians Guild Podcast','The Mystery Hour','The Neurodiverging Podcast','The Official Waiting For Next Year Podcast','The Paper Fold','The Peer Pleasure Podcast','The Planted Runner','The Plug','The Postpartum Coach Podcast','The Pour Over Podcast','The Punk Rock MBA','The Reel Rejects','The Relatable Voice','The Ride or Cry Podcast','The RR Show | Reddit Stories Narrated','The Siecle History Podcast','The Single Mom Podcast','The Smokin Word','The Theatre Podcast with Alan Seales','The Tim Hawkins Podcast','The Tone Mob Podcast','The Vanflip Podcast','The Waiting for Next Year Podcast','The Weekend Show','Theology in the Raw','Thick Skin with Jacque and Hawk','Things I Learned Last Night','This Bitch','This News is So Gay','Thoughts from a Page','Thoughts That Rock','Too Much Effing Perspective','Truck N'' Hustle','True Crime Psychology & Personality Narcissism, Psychopathy','Twin Talks','Two Designers Walk Into a Bar','Unaccountable','Undermine','Unsolved Histories','USMLE Step 1 Success Stories','VoxnHops','Warriors In Their Own Words | First Person War Stories','Watching Two Detectives','We Didn''t Start the Fire The History Podcast','Welsh History Podcast','What Went Wrong','When Dating Hurts','Whine with HR','Whiskey Business','Who Killed...?','Wild Precious Life','Wild West Extravaganza','Women''s Running Stories','Wonder of Parenting','Work Wife Balance','Worship is My Weapon','Worst Possible Timeline','Yoga | Birth | Babies','Zach and Mike Make 3') DEFAULT NULL,
  `side_bonus_percent` decimal(5,2) DEFAULT NULL,
  `youtube_ads_percent` decimal(5,2) DEFAULT NULL,
  `subscriptions_percent` decimal(5,2) DEFAULT NULL,
  `standard_ads_percent` decimal(5,2) DEFAULT NULL,
  `sponsorship_ad_fp_lead_percent` decimal(5,2) DEFAULT NULL,
  `sponsorship_ad_partner_lead_percent` decimal(5,2) DEFAULT NULL,
  `sponsorship_ad_partner_sold_percent` decimal(5,2) DEFAULT NULL,
  `programmatic_ads_span_percent` decimal(5,2) DEFAULT NULL,
  `merchandise_percent` decimal(5,2) DEFAULT NULL,
  `branded_revenue_percent` decimal(5,2) DEFAULT NULL,
  `marketing_services_revenue_percent` decimal(5,2) DEFAULT NULL,
  `direct_customer_hands_off_percent` decimal(5,2) DEFAULT NULL,
  `youtube_hands_off_percent` decimal(5,2) DEFAULT NULL,
  `subscription_hands_off_percent` decimal(5,2) DEFAULT NULL,
  `revenue_2023` decimal(15,2) DEFAULT NULL,
  `revenue_2024` decimal(15,2) DEFAULT NULL,
  `revenue_2025` decimal(15,2) DEFAULT NULL,
  `evergreen_production_staff_name` varchar(50) DEFAULT NULL,
  `show_host_contact` text,
  `show_primary_contact` text,
  PRIMARY KEY (`id`),
  KEY `subnetwork_id` (`subnetwork_id`),
  KEY `genre_name` (`genre_name`),
  CONSTRAINT `shows_ibfk_1` FOREIGN KEY (`subnetwork_id`) REFERENCES `subnetwork` (`id`),
  CONSTRAINT `shows_ibfk_2` FOREIGN KEY (`genre_name`) REFERENCES `genre` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `shows`
--

LOCK TABLES `shows` WRITE;
/*!40000 ALTER TABLE `shows` DISABLE KEYS */;
INSERT INTO `shows` VALUES ('00863280-cd65-42f1-856b-462b5cca8700','Invisible Choir',NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,0,0,0,0,0,0,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),('18700f93-3e66-4b82-a5b3-ba367d847848','Five Minute News',0.00,NULL,'10369345-8eb7-4917-ad70-99ec8e3f39b7','both',1,'strong','Original',0.300,1,1,1,0,0,0,'edbb1d71-4504-4ee9-9c9e-d6c63a469978',1,400,14.00,4,30,'2019-07-01',NULL,1.00,0.88,1.00,0.88,0.88,0.88,1.00,0.88,1.00,0.00,0.00,0.50,0.50,0.50,3798.00,5769.00,10496.00,'none','Anthony Davis, 5 Replace Lane, California, 12345, 323-536-3629, adavis@evergreenpodcasts.com','Anthony Davis, 5 Replace Lane, California, 12345, 323-536-3629, adavis@evergreenpodcasts.com'),('3798563b-bb43-408b-9d9f-7268d9eeea53','American Criminal',NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,0,0,0,0,0,0,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),('4f58dde7-b749-4341-a5b7-d548c3bd281a','Banking Transformed',0.00,NULL,'10369345-8eb7-4917-ad70-99ec8e3f39b7','both',1,'strong','Branded',0.000,1,1,1,1,0,0,'2b766031-3104-45e9-ab97-3a16cccd0b6a',0,400,25.00,4,45,'2019-01-10',NULL,1.00,0.80,1.00,0.80,0.80,0.80,1.00,0.80,1.00,0.00,0.00,0.50,0.50,0.50,44157.00,22200.00,25000.00,'Leah Haslage','Jim Marous, 5 Replace Lane, Ohio 12345, 555-555-5555, jmarous@thefinancialbrand.com','Jim Marous, 5 Replace Lane, Ohio 12345, 555-555-5555, jmarous@thefinancialbrand.com'),('79f9b6db-fced-497d-92af-f30919b24feb','Disturbed',0.00,NULL,'10369345-8eb7-4917-ad70-99ec8e3f39b7','audio',1,'strong','Original',1.000,1,0,1,0,0,0,'45d5bfd2-2b57-455d-9f6c-8fca29092a0d',1,100,24.00,6,40,'2020-01-05',NULL,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,92662.00,45716.00,'Declan Rohrs','Internal','Internal'),('cf5878b0-5b5e-4ce9-8378-7b197a0985c6','Her Money',NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,0,0,0,0,0,0,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
/*!40000 ALTER TABLE `shows` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `subnetwork`
--

DROP TABLE IF EXISTS `subnetwork`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `subnetwork` (
  `id` char(36) NOT NULL,
  `name` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `subnetwork`
--

LOCK TABLES `subnetwork` WRITE;
/*!40000 ALTER TABLE `subnetwork` DISABLE KEYS */;
INSERT INTO `subnetwork` VALUES ('10369345-8eb7-4917-ad70-99ec8e3f39b7','none');
/*!40000 ALTER TABLE `subnetwork` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `id` char(36) NOT NULL,
  `name` text,
  `email` varchar(255) DEFAULT NULL,
  `password_hash` text,
  `role` enum('admin','partner') DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
/* password adminpassword */
INSERT INTO `users` VALUES ('ed69fe69259228685fe7823467094b74','Admin User','admin@evergreen.com','$2b$12$f5drjCZDwHYi8f1avUidDOoAvzEPBToPlspf.3tXxtzYZQoJYvhVC','admin','2025-07-20 13:33:46');
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-07-19 18:27:32
