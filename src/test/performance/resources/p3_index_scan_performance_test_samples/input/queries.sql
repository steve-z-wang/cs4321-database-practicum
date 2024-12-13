SELECT * FROM Boats WHERE Boats.E > 5000;
SELECT * FROM Boats, Sailors WHERE Boats.E > 9500 AND Sailors.A <= 500;
SELECT * FROM Boats, Sailors WHERE Boats.E >= 5000 AND Boats.E < 2000 AND Sailors.A > 5000;