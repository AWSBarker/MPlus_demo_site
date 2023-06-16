-- phpMyAdmin SQL Dump
-- version 4.9.5deb2
-- https://www.phpmyadmin.net/
--
-- Host: awsbarker.ddns.net:3306
-- Generation Time: Jun 11, 2022 at 09:13 AM
-- Server version: 10.1.48-MariaDB-0+deb9u1
-- PHP Version: 7.4.3

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `Mtest3`
--

-- --------------------------------------------------------

--
-- Table structure for table `accounts_customuser`
--

CREATE TABLE `accounts_customuser` (
  `id` bigint(20) NOT NULL,
  `password` varchar(128) COLLATE latin1_general_ci NOT NULL,
  `last_login` datetime(6) DEFAULT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `username` varchar(150) COLLATE latin1_general_ci NOT NULL,
  `first_name` varchar(150) COLLATE latin1_general_ci NOT NULL,
  `last_name` varchar(150) COLLATE latin1_general_ci NOT NULL,
  `email` varchar(254) COLLATE latin1_general_ci NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `date_joined` datetime(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

--
-- Dumping data for table `accounts_customuser`
--

INSERT INTO `accounts_customuser` (`id`, `password`, `last_login`, `is_superuser`, `username`, `first_name`, `last_name`, `email`, `is_staff`, `is_active`, `date_joined`) VALUES
(1, 'pbkdf2_sha256$320000$huBdkOHsWop30xjRfJCqYx$6IgrCFQmfziXRmrW8ZPY8u+Nyyo9CQi8Y6HU4Cj2ZIg=', '2022-06-10 19:51:14.484366', 1, 'Andrew', '', '', 'barker@bluewin.ch', 1, 1, '2022-01-25 18:30:32.847000'),
(2, 'pbkdf2_sha256$320000$eXDtU9uupR01KRQtrOAmBC$LkDM7sTrqttk4G9oKkEOsU6F6HiF46qXO6kc8/pgIPQ=', NULL, 0, 'AB', 'Andrew', 'Barker', 'andrew.barker@medisante-group.com', 0, 1, '2022-01-25 22:02:53.000000'),
(3, 'pbkdf2_sha256$320000$5BYGMF8rdf0FqUAKaqiBB3$9nulERzR2j3XdOAwBYNQyKDs6jDQYqahekJlnJBGsk8=', NULL, 0, 'LC', 'Lionel', 'Cavalliere', 'lionel.cavalliere@medisante-group.com', 1, 1, '2022-01-22 15:09:22.000000'),
(4, 'pbkdf2_sha256$320000$9i9y0JxTBwQiBn3Ly8YpSZ$Ib+AmCgJwDE2zAt3Nu1S6CIbKf+mD1HEmbsfZAmkEJQ=', NULL, 0, 'GQ', 'Gabriela', 'Qi', 'andrew.barker@medisante-group.com', 1, 1, '2022-01-22 15:39:17.000000'),
(5, 'pbkdf2_sha256$320000$m51HsEmX1lXyc99GKTu04o$OmHo0iLiRdzhT0Ci4IkGshsR6THK91ZBa/3Vxzs1BP8=', NULL, 0, 'GL', 'Gilles', 'Lunzenfichter', 'gilles.lunzenfichter@medisante-group.com', 1, 1, '2022-01-22 15:39:53.000000'),
(6, 'pbkdf2_sha256$320000$RSgYcukPuHm5XCOZ85oJ1T$XPjXQIbGwBUUChIjD5wih6LNfAYBCI2CV1Klz0INLuU=', NULL, 0, 'RK', 'Robin', 'Kleiner', 'robin.kleiner@medisante-group.com', 1, 1, '2022-01-22 15:40:10.000000'),
(7, 'pbkdf2_sha256$320000$CGKWZcGhP0rqdwduDzBSk4$QolC+oTJlKRLZ3jUyPp8wE5SXK3qmfi1FqSo+QHzjyk=', NULL, 0, 'A2', 'Andrew', 'Barker', 'andrew.barker@medisante-group.com', 0, 1, '2022-01-22 15:40:36.000000'),
(8, 'pbkdf2_sha256$320000$JYVuwFNHh1wpWK3KszWill$c8y49v7ahPPdIewNuAniri2tqXRISc9F+IO77On04q4=', NULL, 0, 'AR', 'Andrea', 'Rossi', 'andrea.rossi@medisante-group.com', 1, 1, '2022-01-22 15:40:55.000000'),
(9, 'pbkdf2_sha256$320000$xBk1p5mhZwzn22fvoF0BuG$BGSEWXWdJPwigQ6z6QT/9uSJuFg+aUeBiVkbrz+dopQ=', NULL, 0, 'MS', '', '', '', 0, 1, '2022-01-22 15:41:12.562284'),
(99, 'pbkdf2_sha256$320000$xBk1p5mhZwzn22fvoF0BuG$BGSEWXWdJPwigQ6z6QT/9uSJuFg+aUeBiVkbrz+dopQ=', NULL, 0, '', 'null', 'null', '', 0, 1, '2022-01-22 15:41:12.562284');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `accounts_customuser`
--
ALTER TABLE `accounts_customuser`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `username` (`username`),
  ADD UNIQUE KEY `username_2` (`username`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `accounts_customuser`
--
ALTER TABLE `accounts_customuser`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=100;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
