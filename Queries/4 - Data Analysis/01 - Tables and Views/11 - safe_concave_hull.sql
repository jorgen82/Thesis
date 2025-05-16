/*
  This function tests if the creation of Concave Hull is possible, otherwise it will return the Convex Hull.
  In cases thet the points are less than 3, or are very close spatially, then the Concave Hull will fail.
  Convex Hull on the other side is mire robust.
*/

CREATE OR REPLACE FUNCTION data_analysis.safe_concave_hull(geom_collection geometry, concavity float)
RETURNS geometry AS $$
BEGIN
    BEGIN
        -- Try to compute concave hull
        RETURN ST_ConcaveHull(geom_collection, concavity);
    EXCEPTION WHEN OTHERS THEN
        -- Fall back to convex hull if concave fails
        RETURN ST_ConvexHull(geom_collection);
    END;
END;
$$ LANGUAGE plpgsql;
