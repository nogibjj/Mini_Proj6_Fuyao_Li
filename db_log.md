```sql
SELECT * FROM CityDB
```

```sql
INSERT INTO ServeTimesDB VALUES 
        (6/27/2018, Durham South, Durham, NC, 35.99, 78.9);
```

```sql
INSERT INTO ServeTimesDB VALUES 
        (10/22/2018, Charlotte North, Charlotte, NC, 35.22, 80.84);
```

```sql
UPDATE ServeTimesDB SET 
        date=8/29/2018, location=Durham South,
        city=Durham, state=NC, 
        lat=35.99, lng=78.9
        WHERE id=107;
```

```sql
DELETE FROM CityDB WHERE id=99;
```

```sql
SELECT * FROM CityDB
```

