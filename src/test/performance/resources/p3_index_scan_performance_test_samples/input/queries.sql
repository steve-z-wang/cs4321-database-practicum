SELECT * FROM Boats WHERE Boats.E > 1000 AND Boats.E < 9000;
SELECT * FROM Boats WHERE Boats.E > 5000 AND Boats.E < 6000;
SELECT * FROM Boats, Sailors WHERE Boats.E > 9000 AND Sailors.A <= 1000;
SELECT * FROM Boats, Sailors WHERE Boats.E > 8000 AND Sailors.A <= 2000;
