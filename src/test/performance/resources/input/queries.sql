SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND S.B = R.G AND R.H = B.D;