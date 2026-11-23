-- Audit D-3: dependencies were stored comma-joined, so a value containing a
-- comma came back as two. Store them as text[]. Existing rows are split on
-- commas, as the old reader did; a value that was already corrupted on the
-- way in cannot be recovered here. ALTER COLUMN ... USING cannot take a
-- subquery, so the array is built in a new column.
ALTER TABLE services ADD COLUMN dependencies_list TEXT[] NOT NULL DEFAULT '{}';
UPDATE services
SET dependencies_list = ARRAY(
    SELECT btrim(part)
    FROM unnest(string_to_array(dependencies, ',')) WITH ORDINALITY AS t(part, n)
    WHERE btrim(part) <> ''
    ORDER BY n
);
ALTER TABLE services DROP COLUMN dependencies;
ALTER TABLE services RENAME COLUMN dependencies_list TO dependencies;
