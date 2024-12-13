-- Basic range operations on indexed columns
SELECT * FROM Boats WHERE Boats.E < 1000;
SELECT * FROM Boats WHERE Boats.E <= 2000;
SELECT * FROM Boats WHERE Boats.E > 3000;
SELECT * FROM Boats WHERE Boats.E >= 4000;

-- Reverse order comparison operations
SELECT * FROM Boats WHERE 5000 < Boats.E;
SELECT * FROM Boats WHERE 6000 <= Boats.E;
SELECT * FROM Boats WHERE 7000 > Boats.E;
SELECT * FROM Boats WHERE 8000 >= Boats.E;

-- Point queries (exact matches)
SELECT * FROM Boats WHERE Boats.E = 500;

-- Non-indexed column operations
SELECT * FROM Boats WHERE Boats.F < 1000;
SELECT * FROM Boats WHERE Boats.D > 2000 AND Boats.E <= 3000;
SELECT * FROM Boats WHERE Boats.F > 3000 AND Boats.D <= 4000;

-- Range queries on same indexed column
SELECT * FROM Boats WHERE Boats.E > 1000 AND Boats.E <= 2000;
SELECT * FROM Boats WHERE Boats.E >= 1000 AND Boats.E < 2000;
SELECT * FROM Boats WHERE Boats.E > 5000 AND Boats.D < 1000;

-- Index operations on Sailors table
SELECT * FROM Sailors WHERE Sailors.A < 1000;
SELECT * FROM Sailors WHERE Sailors.A > 1000;
SELECT * FROM Sailors WHERE Sailors.A > 5000 AND Sailors.A <= 6000;
SELECT * FROM Sailors WHERE Sailors.A < 5000 AND Sailors.B > 2000;

-- Join queries with index conditions
SELECT * FROM Boats, Sailors WHERE Boats.E > 9500 AND Sailors.A <= 500;
SELECT * FROM Boats, Sailors WHERE Boats.E < 1000 AND Sailors.A > 9000;
SELECT * FROM Boats, Sailors WHERE Boats.E >= 5000 AND Boats.E > 2000 AND Sailors.A > 5000;