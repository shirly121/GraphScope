use std::cmp::Ordering;

/// Intersecting two sorted arrays.
#[inline(always)]
pub fn intersect<T: Copy + Ord>(
    aaa: &[T],
    mut bbb: &[T],
    mut results: Option<&mut Vec<T>>,
) -> usize {
    let mut count = 0;

    // magic gallop overhead # is 4
    if aaa.len() < bbb.len() / 4 {
        for a in aaa {
            bbb = gallop(bbb, a);
            if !bbb.is_empty() && &bbb[0] == a {
                count += 1;
                if let Some(vec) = results.as_mut() {
                    vec.push(*a);
                }
            }
        }
    } else {
        for &a in aaa {
            while !bbb.is_empty() && bbb[0] < a {
                bbb = &bbb[1..];
            }
            if !bbb.is_empty() && a == bbb[0] {
                count += 1;
                if let Some(vec) = results.as_mut() {
                    vec.push(a);
                }
            }
        }
    }
    count
}

/// The `gallop` binary searching algorithm.
/// **Note** it is necessary to guarantee that `slice` is sorted.
///
/// # Parameters
///
/// * `slice`: The ordered slice to search in.
/// * `cmp`: A udf comparing function, which indicates `key.cmp(&slice[i])`
///
/// # Return
///
/// A sub-slice `SL` of the input slice such that `cmp(SL[0]) != Ordering::Greater`.
///
/// # Example
///
/// ```
/// extern crate patmat_bench;
///
/// use patmat_bench::utils::gallop_by;
///
/// let vec = vec![1, 2, 3];
/// let key = 2;
///
/// let slice = gallop_by(&vec, |x: &u32| key.cmp(x));
/// assert_eq!(slice.to_vec(), vec![2, 3]);
/// ```
#[inline(always)]
pub fn gallop_by<T>(mut slice: &[T], mut cmp: impl FnMut(&T) -> Ordering) -> &[T] {
    // if empty slice, or key <= slice[0] already, return
    if !slice.is_empty() && cmp(&slice[0]) == Ordering::Greater {
        let mut step = 1;
        while step < slice.len() && cmp(&slice[step]) == Ordering::Greater {
            slice = &slice[step..];
            step <<= 1;
        }

        step >>= 1;
        while step > 0 {
            if step < slice.len() && cmp(&slice[step]) == Ordering::Greater {
                slice = &slice[step..];
            }
            step >>= 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed < key
    }

    slice
}

#[inline(always)]
pub fn gallop_gt_by<T>(mut slice: &[T], mut cmp: impl FnMut(&T) -> Ordering + Copy) -> &[T] {
    slice = gallop_by(slice, cmp);

    while !slice.is_empty() && cmp(&slice[0]) == Ordering::Equal {
        slice = &slice[1..];
    }

    slice
}

/// The `gallop` algorithm with key.
#[inline(always)]
pub fn gallop<'a, T: Ord>(slice: &'a [T], key: &T) -> &'a [T] {
    gallop_by(slice, |x: &T| key.cmp(x))
}

/// The `gallop_gt` algorithm with key
#[inline(always)]
pub fn gallop_gt<'a, T: Ord>(slice: &'a [T], key: &T) -> &'a [T] {
    gallop_gt_by(slice, |x: &T| key.cmp(x))
}
