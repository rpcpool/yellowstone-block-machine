use {
    rustc_hash::{FxHashMap, FxHashSet},
    solana_clock::Slot,
    std::{
        collections::{BTreeSet, HashSet, VecDeque},
        hash::Hash,
    },
};

///
/// Efficient Ordered Set implementatoin that keeps track of the order of insertions and deletions.
///
/// Every operation is O(1).
///
#[derive(Default, Debug, Clone)]
pub struct OrderedSet<K> {
    versioned_keys: FxHashMap<K, u64>,
    order: VecDeque<(K, u64)>,
    deleted: FxHashSet<u64>,
    version: u64,
    len: usize,
}

impl<K> OrderedSet<K>
where
    K: Clone + Hash + Eq,
{
    pub fn new() -> Self {
        Self {
            versioned_keys: Default::default(),
            order: Default::default(),
            deleted: Default::default(),
            version: 0,
            len: 0,
        }
    }

    fn next_version(&mut self) -> u64 {
        let version = self.version;
        self.version += 1;
        version
    }

    pub fn first(&self) -> Option<&K> {
        self.order
            .iter()
            .find(|(_, version)| !self.deleted.contains(version))
            .map(|(key, _)| key)
    }

    ///
    /// Insert a key into the set.
    ///
    /// Returns true if the key was already present in the set.
    pub fn insert(&mut self, key: K) -> bool {
        let is_present = match self.versioned_keys.get(&key) {
            Some(version) => !self.deleted.contains(version),
            _ => false,
        };

        if is_present {
            return true;
        }

        let next_version = self.next_version();
        self.versioned_keys.insert(key.clone(), next_version);
        self.order.push_back((key, next_version));
        self.len += 1;
        false
    }

    ///
    /// Pops the next element inserted into the set.
    ///
    /// Returns None if the set is empty.
    pub fn pop_next(&mut self) -> Option<K> {
        loop {
            match self.order.pop_front() {
                Some((key, version)) => {
                    if !self.deleted.remove(&version) {
                        self.versioned_keys.remove(&key);
                        self.len -= 1;
                        return Some(key);
                    } else {
                        continue;
                    }
                }
                _ => {
                    return None;
                }
            }
        }
    }

    ///
    /// Removes a key from the set.
    ///
    /// Returns true if the key was present in the set.
    pub fn remove(&mut self, key: &K) -> bool {
        match self.versioned_keys.remove(key) {
            Some(version) => {
                self.deleted.insert(version);
                self.len -= 1;
                true
            }
            _ => false,
        }
    }

    ///
    /// Returns true if the set is empty.
    ///
    pub fn is_empty(&self) -> bool {
        self.versioned_keys.is_empty()
    }

    ///
    /// Returns true if the set contains the key.
    ///
    pub fn contains(&self, key: &K) -> bool {
        self.versioned_keys.contains_key(key)
    }

    ///
    /// Returns an iterator over the keys in the set.
    ///
    pub fn iter(&self) -> OrderedSetIter<K> {
        OrderedSetIter {
            ordered_set: self,
            next_index: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

pub struct OrderedSetIntoIterator<K> {
    ordered_set: OrderedSet<K>,
}

impl<K> Iterator for OrderedSetIntoIterator<K>
where
    K: Clone + Hash + Eq,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.ordered_set.pop_next()
    }
}

impl<K> IntoIterator for OrderedSet<K>
where
    K: Clone + Hash + Eq,
{
    type Item = K;
    type IntoIter = OrderedSetIntoIterator<K>;

    fn into_iter(self) -> Self::IntoIter {
        OrderedSetIntoIterator { ordered_set: self }
    }
}

///
/// Iterator for [`OrderedSet`].
///
/// Iterates over the keys in the order they were originally inserted.
///  
pub struct OrderedSetIter<'a, K> {
    ordered_set: &'a OrderedSet<K>,
    next_index: usize,
}

impl<'a, K> Iterator for OrderedSetIter<'a, K>
where
    K: Clone + Hash + Eq,
{
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.ordered_set.order.get(self.next_index) {
                Some((key, version)) => {
                    self.next_index += 1;
                    if !self.ordered_set.deleted.contains(version) {
                        return Some(key);
                    } else {
                        continue;
                    }
                }
                _ => {
                    return None;
                }
            }
        }
    }
}

///
/// Forks is a utility to detect forks in a blockchain.
/// Unlike the ForkBanks in Solana runtmie, this struct is more of a utility to detect forks in a blockchain incrementally.
/// It retroactively detects forks in a blockchain.
#[derive(Debug)]
pub struct Forks {
    parent_children_map: FxHashMap<Slot /*parent */, BTreeSet<Slot> /*children */>,
    rooted_slots: BTreeSet<Slot>,
    // Map from child to parent
    reverse_parent_children_map: FxHashMap<Slot /*child */, Slot /* parent */>,
    forked_slots: FxHashSet<Slot>,
    // Maximum number of rooted slots to keep track of.
    max_rooted_depth_capacity: usize,
}

impl Default for Forks {
    fn default() -> Self {
        Self::with_max_capacity(1000)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ForksCapacityStatus {
    BelowCapacity,
    AtCapacity,
    AboveCapacity,
}

pub struct ForksIterator<'forks> {
    forks: &'forks Forks,
    to_visit: FxHashSet<Slot>,
    visited: FxHashSet<Slot>,
    queue: VecDeque<Slot>,
}

impl Iterator for ForksIterator<'_> {
    type Item = Slot;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.to_visit.is_empty() {
            while let Some(slot) = self.queue.pop_front() {
                if self.visited.contains(&slot) {
                    continue;
                }
                self.visited.insert(slot);
                self.to_visit.remove(&slot);

                if let Some(children) = self.forks.parent_children_map.get(&slot) {
                    for child in children.iter().copied() {
                        if !self.visited.contains(&child) {
                            self.queue.push_back(child);
                        }
                    }
                }
                return Some(slot);
            }

            if let Some(slot) = self.to_visit.iter().copied().next() {
                self.queue.push_back(slot);
            }
        }
        None
    }
}

///
/// Trait for tracing forks update during mutations of the Forks structure.
///
pub trait ForksMutationTracer {
    fn insert(&mut self, slot: Slot);

    fn extend(&mut self, slots: impl IntoIterator<Item = Slot>) {
        for slot in slots {
            self.insert(slot);
        }
    }
}

impl ForksMutationTracer for FxHashSet<Slot> {
    fn insert(&mut self, slot: Slot) {
        self.insert(slot);
    }

    fn extend(&mut self, slots: impl IntoIterator<Item = Slot>) {
        Extend::extend(self, slots);
    }
}

impl ForksMutationTracer for HashSet<Slot> {
    fn insert(&mut self, slot: Slot) {
        self.insert(slot);
    }

    fn extend(&mut self, slots: impl IntoIterator<Item = Slot>) {
        Extend::extend(self, slots);
    }
}

impl ForksMutationTracer for Vec<Slot> {
    fn insert(&mut self, slot: Slot) {
        if self.contains(&slot) {
            return;
        }
        self.push(slot);
    }

    fn extend(&mut self, slots: impl IntoIterator<Item = Slot>) {
        for slot in slots {
            if self.contains(&slot) {
                continue;
            }
            self.push(slot);
        }
    }
}

impl ForksMutationTracer for BTreeSet<Slot> {
    fn insert(&mut self, slot: Slot) {
        self.insert(slot);
    }

    fn extend(&mut self, slots: impl IntoIterator<Item = Slot>) {
        Extend::extend(self, slots);
    }
}

///
/// No-op implementation of ForksMutationTrace.
///
pub struct NoTrace;

impl ForksMutationTracer for NoTrace {
    fn insert(&mut self, _slot: Slot) {
        // Do nothing
    }
}

impl Forks {
    pub fn with_max_capacity(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            parent_children_map: Default::default(),
            reverse_parent_children_map: Default::default(),
            rooted_slots: Default::default(),
            forked_slots: Default::default(),
            max_rooted_depth_capacity: capacity,
        }
    }

    pub fn oldest_rooted_slot(&self) -> Option<Slot> {
        self.rooted_slots.first().copied()
    }

    pub fn capacity_status(&self) -> ForksCapacityStatus {
        match self.rooted_slots.len().cmp(&self.max_rooted_depth_capacity) {
            std::cmp::Ordering::Less => ForksCapacityStatus::BelowCapacity,
            std::cmp::Ordering::Equal => ForksCapacityStatus::AtCapacity,
            std::cmp::Ordering::Greater => ForksCapacityStatus::AboveCapacity,
        }
    }

    ///
    /// Remove the old rooted slot and all forks derived from it.
    fn pop_oldest_rooted_slot(&mut self, forks_removed: &mut FxHashSet<Slot>) {
        if let Some(root) = self.rooted_slots.pop_first() {
            let child = self.parent_children_map.remove(&root).unwrap_or_default();
            let mut queue = VecDeque::from_iter(child);

            while !queue.is_empty() {
                let slot = queue.pop_front().unwrap();
                if !self.forked_slots.contains(&slot) {
                    continue;
                }
                forks_removed.insert(slot);
                self.reverse_parent_children_map.remove(&slot);
                self.forked_slots.remove(&slot);

                if let Some(children) = self.parent_children_map.remove(&slot) {
                    queue.extend(children.into_iter());
                }
            }

            if let Some(new_root) = self.rooted_slots.first().copied() {
                self.reverse_parent_children_map.remove(&new_root);
            }
        }
    }

    pub fn truncate_excess_rooted_slots(&mut self, forks_removed: &mut FxHashSet<Slot>) {
        while self.capacity_status() == ForksCapacityStatus::AboveCapacity {
            self.pop_oldest_rooted_slot(forks_removed);
        }
    }

    fn get_all_nodes(&self) -> FxHashSet<Slot> {
        self.parent_children_map.keys().copied().collect()
    }

    pub fn get_parent(&self, slot: &Slot) -> Option<Slot> {
        self.reverse_parent_children_map.get(slot).copied()
    }

    pub fn visit_slots(&self) -> ForksIterator {
        ForksIterator {
            forks: self,
            to_visit: self.get_all_nodes(),
            visited: Default::default(),
            queue: VecDeque::from_iter(self.oldest_rooted_slot()),
        }
    }
    #[allow(clippy::collapsible_else_if)]
    fn mark_children_as_forks(&mut self, slot: Slot) -> FxHashSet<Slot> {
        let mut queue = VecDeque::from([slot]);
        let mut newly_forked_slots = FxHashSet::default();
        let mut visited = FxHashSet::default();
        while !queue.is_empty() {
            let slot2 = queue.pop_front().unwrap();
            if !visited.insert(slot2) {
                continue;
            }

            if let Some(children) = self.parent_children_map.get(&slot2) {
                for child in children.iter().copied() {
                    if self.rooted_slots.contains(&child) {
                        continue;
                    } else {
                        if self.forked_slots.insert(child) {
                            queue.push_back(child);
                            newly_forked_slots.insert(child);
                        }
                    }
                }
            }
        }
        newly_forked_slots
    }

    pub fn is_rooted_slot(&self, slot: &Slot) -> bool {
        self.rooted_slots.contains(slot)
    }

    pub fn make_slot_rooted_with_rooted_trace<T1, T2>(
        &mut self,
        slot: Slot,
        newly_forked_slot_out: &mut T1,
        indirectly_rooted_slots: &mut T2,
    ) where
        T1: ForksMutationTracer,
        T2: ForksMutationTracer,
    {
        if self.rooted_slots.contains(&slot) {
            return;
        }
        self.rooted_slots.insert(slot);

        // The rest of the function retroactively detect forks of slot's sibblings, cousins and great* cousins.
        let mut queue = VecDeque::new();
        if let Some(parent) = self.reverse_parent_children_map.get(&slot) {
            queue.push_back(*parent);
        }
        while !queue.is_empty() {
            let slot2 = queue.pop_front().expect("empty");
            // this line will only work the first iteration in the case
            // we mark a slot i "rooted" and its parent is not rooted yet.
            if self.rooted_slots.insert(slot2) {
                indirectly_rooted_slots.insert(slot2);
            }

            newly_forked_slot_out.extend(self.mark_children_as_forks(slot2));

            if let Some(parent2) = self.reverse_parent_children_map.get(&slot2) {
                // If the parent is already rooted, we don't need to mark its children as forks.
                // because this process must have been done in the past.
                if !self.rooted_slots.insert(*parent2) {
                    continue;
                } else {
                    indirectly_rooted_slots.insert(*parent2);
                }
                queue.push_back(*parent2);
            }
        }
    }

    pub fn mark_slot_as_forked<T>(&mut self, slot: Slot, forks_detected: &mut T)
    where
        T: ForksMutationTracer,
    {
        self.parent_children_map.entry(slot).or_default();
        if self.forked_slots.insert(slot) {
            forks_detected.insert(slot);
        }
        forks_detected.extend(self.mark_children_as_forks(slot))
    }

    pub fn make_slot_rooted<T>(&mut self, slot: Slot, newly_forked_slot_out: &mut T)
    where
        T: ForksMutationTracer,
    {
        self.make_slot_rooted_with_rooted_trace(slot, newly_forked_slot_out, &mut NoTrace);
    }

    #[allow(clippy::collapsible_if)]
    pub fn add_slot_with_parent_with_rooted_trace<T1, T2>(
        &mut self,
        slot: Slot,
        parent: Slot,
        newly_forked_slot_out: &mut T1,
        indireclty_rooted: &mut T2,
    ) -> bool
    where
        T1: ForksMutationTracer,
        T2: ForksMutationTracer,
    {
        if self
            .parent_children_map
            .entry(parent)
            .or_default()
            .contains(&slot)
        {
            // If the slot is already present, we don't need to do anything.
            return false;
        }

        let mut parent_is_rooted = false;
        if self.rooted_slots.contains(&parent) {
            parent_is_rooted = true;
            // If the parent is rooted, we must make sure that none of its children is rooted before inserting.
            // Otherwise slot is a fork
            let sibblings = self.parent_children_map.entry(parent).or_default();
            for sibbling in sibblings.iter() {
                if sibbling == &slot {
                    break;
                }
                if self.rooted_slots.contains(sibbling) {
                    // Parent is rooted so is one of its children.
                    // This mean that `slot` is a fork
                    if self.forked_slots.insert(slot) {
                        newly_forked_slot_out.insert(slot);
                    }
                }
            }
        }

        self.parent_children_map
            .entry(parent)
            .or_default()
            .insert(slot);
        self.parent_children_map.entry(slot).or_default();
        self.reverse_parent_children_map.insert(slot, parent);

        let is_rooted = self.is_rooted_slot(&slot);

        match (is_rooted, parent_is_rooted) {
            (true, false) => {
                indireclty_rooted.insert(parent);
                self.make_slot_rooted_with_rooted_trace(
                    parent,
                    newly_forked_slot_out,
                    indireclty_rooted,
                )
            }
            (false, false) => {
                if self.forked_slots.contains(&parent) {
                    if self.forked_slots.insert(slot) {
                        newly_forked_slot_out.insert(slot);
                    }
                }
            }
            _ => {}
        }
        true
    }

    pub fn add_slot_with_parent<T>(
        &mut self,
        slot: Slot,
        parent: Slot,
        newly_forked_slot_out: &mut T,
    ) -> bool
    where
        T: ForksMutationTracer,
    {
        self.add_slot_with_parent_with_rooted_trace(
            slot,
            parent,
            newly_forked_slot_out,
            &mut NoTrace,
        )
    }

    pub fn len(&self) -> usize {
        self.parent_children_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parent_children_map.is_empty()
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn module_path_for_test() -> &'static str {
    module_path!()
}

#[cfg(test)]
mod orderset_tests {
    use super::*;

    #[test]
    fn pop_first_should_be_fifo_semantic() {
        let mut ordered_set = OrderedSet::new();
        assert!(ordered_set.is_empty());
        assert!(!ordered_set.insert(1));
        assert!(!ordered_set.insert(2));
        assert!(!ordered_set.insert(3));
        assert!(!ordered_set.insert(4));

        assert_eq!(ordered_set.pop_next(), Some(1));
        assert_eq!(ordered_set.pop_next(), Some(2));
        assert_eq!(ordered_set.pop_next(), Some(3));
        assert_eq!(ordered_set.pop_next(), Some(4));
        assert_eq!(ordered_set.pop_next(), None);
    }

    #[test]
    fn pop_first_should_handle_deleted_elements() {
        let mut ordered_set = OrderedSet::new();
        assert!(ordered_set.is_empty());
        assert!(!ordered_set.insert(1));
        assert!(!ordered_set.insert(2));
        assert!(!ordered_set.insert(3));
        assert!(!ordered_set.insert(4));

        assert!(ordered_set.remove(&2));
        assert!(!ordered_set.contains(&2));

        assert_eq!(ordered_set.pop_next(), Some(1));
        assert_eq!(ordered_set.pop_next(), Some(3));
        assert_eq!(ordered_set.pop_next(), Some(4));
        assert_eq!(ordered_set.pop_next(), None);
    }

    #[test]
    fn pop_first_should_return_none_if_set_empty() {
        let mut ordered_set = OrderedSet::<i32>::new();
        assert!(ordered_set.is_empty());
        assert_eq!(ordered_set.pop_next(), None);
    }

    #[test]
    fn insert_should_return_true_if_key_already_present() {
        let mut ordered_set = OrderedSet::new();
        assert!(ordered_set.is_empty());
        assert!(!ordered_set.insert(1));
        assert!(ordered_set.insert(1));
    }

    #[test]
    fn empty_ordereset_iter_should_return_none() {
        let ordered_set = OrderedSet::<i32>::new();
        let mut iter = ordered_set.iter();
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn all_popped_orderedset_iter_should_return_none() {
        let mut ordered_set = OrderedSet::new();
        assert!(ordered_set.is_empty());
        assert!(!ordered_set.insert(1));
        assert!(!ordered_set.insert(2));
        assert!(!ordered_set.insert(3));
        assert!(!ordered_set.insert(4));

        assert_eq!(ordered_set.pop_next(), Some(1));
        assert_eq!(ordered_set.pop_next(), Some(2));
        assert_eq!(ordered_set.pop_next(), Some(3));
        assert_eq!(ordered_set.pop_next(), Some(4));
        assert_eq!(ordered_set.pop_next(), None);

        let mut iter = ordered_set.iter();
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn ordereset_iter_should_ignore_removed_elements() {
        let mut ordered_set = OrderedSet::new();
        assert!(ordered_set.is_empty());
        assert!(!ordered_set.insert(1));
        assert!(!ordered_set.insert(2));
        assert!(!ordered_set.insert(3));
        assert!(!ordered_set.insert(4));

        assert!(ordered_set.remove(&2));
        assert!(!ordered_set.contains(&2));

        let mut iter = ordered_set.iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&4));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn orderset_iter_should_go_through_all_elements() {
        let mut ordered_set = OrderedSet::new();
        assert!(ordered_set.is_empty());
        assert!(!ordered_set.insert(1));
        assert!(!ordered_set.insert(2));
        assert!(!ordered_set.insert(3));
        assert!(!ordered_set.insert(4));

        let mut iter = ordered_set.iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&4));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn removing_then_reinsert_should_change_the_set_ordering() {
        let mut ordered_set = OrderedSet::new();
        assert!(ordered_set.is_empty());
        assert!(!ordered_set.insert(1));
        assert!(!ordered_set.insert(2));
        assert!(!ordered_set.insert(3));
        assert!(!ordered_set.insert(4));

        assert!(ordered_set.remove(&2));
        assert!(!ordered_set.contains(&2));

        assert!(!ordered_set.insert(2));
        assert!(ordered_set.contains(&2));

        let mut iter = ordered_set.iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&4));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);

        let x = ordered_set.pop_next();
        assert_eq!(x, Some(1));
        let x = ordered_set.pop_next();
        assert_eq!(x, Some(3));
        let x = ordered_set.pop_next();
        assert_eq!(x, Some(4));
        let x = ordered_set.pop_next();
        assert_eq!(x, Some(2));
        let x = ordered_set.pop_next();
        assert_eq!(x, None);
        assert!(ordered_set.is_empty());
    }
}

#[cfg(test)]
mod forks_tests {
    use {crate::forks::Forks, rustc_hash::FxHashSet};

    #[test]
    fn adding_slot_with_parent_twice_should_shortcut() {
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        assert!(fd.add_slot_with_parent(2, 1, &mut forked_detected));
        assert!(!fd.add_slot_with_parent(2, 1, &mut forked_detected));
        assert!(forked_detected.is_empty());
    }

    #[test]
    fn rooting_slot_should_be_retroactive() {
        // test case 0
        // 1 (rooted) -> 2 -> 3 -> 4
        // make 4 rooted
        // expected rooted: 1, 2, 3, 4
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.make_slot_rooted(1, &mut forked_detected);
        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 3, &mut forked_detected);

        assert!(fd.rooted_slots.contains(&1));
        assert!(fd.rooted_slots.len() == 1);
        let mut indirectly_rooted = FxHashSet::default();
        fd.make_slot_rooted_with_rooted_trace(4, &mut forked_detected, &mut indirectly_rooted);
        assert_eq!(indirectly_rooted.len(), 2);
        assert!(indirectly_rooted.contains(&2));
        assert!(indirectly_rooted.contains(&3));

        assert!(fd.rooted_slots.contains(&1));
        assert!(fd.rooted_slots.contains(&2));
        assert!(fd.rooted_slots.contains(&3));
        assert!(fd.rooted_slots.contains(&4));
        assert!(forked_detected.is_empty());
    }

    #[test]
    fn forks_should_detect_sibblings_forks() {
        // test case 0 (no sibblings)
        // 1 -> 2
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.make_slot_rooted(2, &mut forked_detected);
        assert!(fd.forked_slots.is_empty());
        assert!(fd.is_rooted_slot(&2));
        assert!(fd.is_rooted_slot(&1));

        // test case1 :
        //  1 -> 2
        //  1 -> 3
        // make 2 rooted
        // expected rooted 2, 1
        // expected forks 3
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.add_slot_with_parent(3, 1, &mut forked_detected);
        fd.make_slot_rooted(2, &mut forked_detected);
        assert!(fd.forked_slots.contains(&3));
        assert_eq!(fd.forked_slots.len(), 1);
        assert!(fd.is_rooted_slot(&2));
        assert!(fd.is_rooted_slot(&1));

        // test case 2
        // 1 -> 2 -> 3
        // 1 -> 4
        // make 2 rooted
        // expected rooted: 1, 2
        // expected forks: 4
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 1, &mut forked_detected);
        fd.make_slot_rooted(2, &mut forked_detected);
        assert!(fd.forked_slots.contains(&4));
        assert_eq!(fd.forked_slots.len(), 1);
        assert!(fd.is_rooted_slot(&1));
        assert!(fd.is_rooted_slot(&2));
        assert!(!fd.is_rooted_slot(&3));

        // test case 3
        // 1 -> 2 -> 3
        // 1 -> 4
        // make 4 rooted
        // expected rooted: 1, 4
        // expected forks: 2, 3
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 1, &mut forked_detected);
        fd.make_slot_rooted(4, &mut forked_detected);
        assert!(fd.forked_slots.contains(&2));
        assert!(fd.forked_slots.contains(&3));
        assert_eq!(fd.forked_slots.len(), 2);
        assert!(fd.is_rooted_slot(&1));
        assert!(fd.is_rooted_slot(&4));
        assert!(!fd.is_rooted_slot(&2));
        assert!(!fd.is_rooted_slot(&3));
    }

    #[test]
    fn forks_should_detect_cousins_forks() {
        // test case 1:
        // 1 (rooted) -> 2 -> 5
        // 2 -> 3 -> 4
        // expected rooted : 1,2,5
        // expected forks : 3, 4

        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.make_slot_rooted(1, &mut forked_detected);

        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 3, &mut forked_detected);

        fd.add_slot_with_parent(5, 2, &mut forked_detected);

        fd.make_slot_rooted(5, &mut forked_detected);

        assert!(fd.is_rooted_slot(&1));
        assert!(fd.is_rooted_slot(&2));
        assert!(fd.is_rooted_slot(&5));
        assert!(fd.forked_slots.contains(&3));
        assert!(fd.forked_slots.contains(&4));
        assert!(!fd.is_rooted_slot(&3));
        assert!(!fd.is_rooted_slot(&4));
    }

    #[test]
    fn forks_should_detect_retroactively_greater_cousins_forks() {
        // Initial setup:
        // 1 (rooted) -> 2 -> 5 -> ???
        // 2 -> 3 -> 4
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();

        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.make_slot_rooted(1, &mut forked_detected);

        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 3, &mut forked_detected);

        fd.add_slot_with_parent(5, 2, &mut forked_detected);

        assert!(fd.is_rooted_slot(&1));
        assert!(fd.forked_slots.is_empty());

        // Then we define another rooted segment
        // 9 -> 10
        let mut retroactively_rooted_set = FxHashSet::default();
        fd.add_slot_with_parent(10, 9, &mut forked_detected);
        fd.make_slot_rooted_with_rooted_trace(
            10,
            &mut forked_detected,
            &mut retroactively_rooted_set,
        );
        assert_eq!(retroactively_rooted_set.len(), 1);
        assert!(retroactively_rooted_set.contains(&9));
        assert!(fd.is_rooted_slot(&10));
        assert!(fd.is_rooted_slot(&9));

        assert!(fd.forked_slots.is_empty());

        // then we connect both segment on 5 -> 9
        // this should retroactively make 3,4 forks
        // and make 2 and 5 rooted too
        let mut retroactively_rooted_set = FxHashSet::default();
        fd.add_slot_with_parent_with_rooted_trace(
            9,
            5,
            &mut forked_detected,
            &mut retroactively_rooted_set,
        );
        assert_eq!(retroactively_rooted_set.len(), 2);
        assert!(retroactively_rooted_set.contains(&2));
        assert!(retroactively_rooted_set.contains(&5));
        assert!(fd.is_rooted_slot(&2));
        assert!(fd.is_rooted_slot(&5));
        assert!(fd.forked_slots.contains(&3));
        assert!(fd.forked_slots.contains(&4));
        assert_eq!(fd.forked_slots.len(), 2);
    }

    #[test]
    fn forks_descendant_should_be_forks_aswell() {
        // test case1 :
        //  1 -> 2
        //  1 -> 3
        // make 2 rooted
        // expected rooted 2, 1
        // expected forks 3
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.add_slot_with_parent(3, 1, &mut forked_detected);
        fd.make_slot_rooted(2, &mut forked_detected);
        assert!(fd.forked_slots.contains(&3));
        assert_eq!(fd.forked_slots.len(), 1);
        assert!(fd.is_rooted_slot(&2));
        assert!(fd.is_rooted_slot(&1));

        // add 4 as a descendant of 3
        // expected rooted 2, 1
        // expected forks 3, 4

        fd.add_slot_with_parent(4, 3, &mut forked_detected);
        assert!(fd.forked_slots.contains(&3));
        assert!(fd.forked_slots.contains(&4));
        assert_eq!(fd.forked_slots.len(), 2);
        assert!(fd.is_rooted_slot(&2));
        assert!(fd.is_rooted_slot(&1));
        assert!(forked_detected.contains(&4));

        let all_slots = fd.visit_slots().collect::<FxHashSet<_>>();
        assert_eq!(all_slots.len(), 4);
    }

    #[test]
    fn forks_iterator_should_work_on_disjoint_history() {
        //
        // Disjoint history comes from the fact that the local history may not contain the full history of the blockchain.
        // In this case, the local history is a subset of the full history.
        // Some slots may be missing from the local history making some slots unreachable from oldest known rooted slot.
        //
        // The iterator should be able to traverse the disjoint history.

        //
        // 1 -> 2
        //
        // 4 -> 5
        // (missing 2 ~~> 4 path)
        let mut fd = Forks::default();
        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(1, 2, &mut forked_detected);
        assert!(forked_detected.is_empty());
        fd.add_slot_with_parent(4, 5, &mut forked_detected);
        assert!(forked_detected.is_empty());

        let slots = fd.visit_slots().collect::<FxHashSet<_>>();
        assert_eq!(fd.get_all_nodes().len(), 4);
        assert_eq!(slots.len(), 4);
        assert!(slots.contains(&1));
        assert!(slots.contains(&2));
        assert!(slots.contains(&4));
        assert!(slots.contains(&5));
    }

    #[test]
    fn forks_should_truncate_old_rooted_slot_when_max_history_capacity_is_reached() {
        // test case
        //
        //                             14 -> 15 -> 16 -> 17 -> 18 -> 19 -> 20
        //                             ^
        //                             |
        // 1 (rooted) -> 2 (rooted) -> 3 (rooted) -> 4 (rooted) -> 5 -> 6 -> 7 -> 8 -> 9 -> 10
        // |
        // v
        // 11 -> 12 -> 13
        //
        // Popoing 1 should remove 11, 12, 13 which are forks
        //

        let mut fd = Forks::with_max_capacity(1);

        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 3, &mut forked_detected);
        fd.add_slot_with_parent(5, 4, &mut forked_detected);
        fd.add_slot_with_parent(6, 5, &mut forked_detected);
        fd.add_slot_with_parent(7, 6, &mut forked_detected);
        fd.add_slot_with_parent(8, 7, &mut forked_detected);
        fd.add_slot_with_parent(9, 8, &mut forked_detected);
        fd.add_slot_with_parent(10, 9, &mut forked_detected);

        // 1 -> 11 -> 12 -> 13 (Forked path)
        fd.add_slot_with_parent(11, 1, &mut forked_detected);
        fd.add_slot_with_parent(12, 11, &mut forked_detected);
        fd.add_slot_with_parent(13, 12, &mut forked_detected);

        // 3 -> 14 -> 15 -> 16 -> 17 -> 18 -> 19 -> 20 (Forked path)
        fd.add_slot_with_parent(14, 3, &mut forked_detected);
        fd.add_slot_with_parent(15, 14, &mut forked_detected);
        fd.add_slot_with_parent(16, 15, &mut forked_detected);
        fd.add_slot_with_parent(17, 16, &mut forked_detected);
        fd.add_slot_with_parent(18, 17, &mut forked_detected);
        fd.add_slot_with_parent(19, 18, &mut forked_detected);
        fd.add_slot_with_parent(20, 19, &mut forked_detected);

        fd.make_slot_rooted(1, &mut forked_detected);
        assert_eq!(forked_detected.len(), 0);

        fd.make_slot_rooted(2, &mut forked_detected);

        let expected_forks_detected = FxHashSet::from_iter([11, 12, 13]);
        assert_eq!(forked_detected, expected_forks_detected);

        forked_detected.clear();

        fd.make_slot_rooted(4, &mut forked_detected);
        let expected_forks_detected = FxHashSet::from_iter([14, 15, 16, 17, 18, 19, 20]);
        assert_eq!(forked_detected, expected_forks_detected);

        let mut forks_removed = FxHashSet::default();

        fd.pop_oldest_rooted_slot(&mut forks_removed);

        let expected_deleted_forks = FxHashSet::from_iter([11, 12, 13]);
        assert_eq!(forks_removed, expected_deleted_forks);

        let all_slots = fd.visit_slots().collect::<FxHashSet<_>>();
        assert_eq!(all_slots.len(), 16);

        assert_eq!(fd.oldest_rooted_slot(), Some(2));
    }

    #[test]
    fn forking_treeroot_should_make_whole_tree_forked() {
        // test case
        //
        //          14 -> 15 -> 16 -> 17 -> 18 -> 19 -> 20
        //           ^
        //           |
        // 1 -> 2 -> 3 -> 4  -> 5 -> 6 -> 7 -> 8 -> 9 -> 10
        // |
        // v
        // 11 -> 12 -> 13
        //
        // Forking 1 should mark all slots as forks

        let mut fd = Forks::with_max_capacity(1);

        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 3, &mut forked_detected);
        fd.add_slot_with_parent(5, 4, &mut forked_detected);
        fd.add_slot_with_parent(6, 5, &mut forked_detected);
        fd.add_slot_with_parent(7, 6, &mut forked_detected);
        fd.add_slot_with_parent(8, 7, &mut forked_detected);
        fd.add_slot_with_parent(9, 8, &mut forked_detected);
        fd.add_slot_with_parent(10, 9, &mut forked_detected);

        // 1 -> 11 -> 12 -> 13
        fd.add_slot_with_parent(11, 1, &mut forked_detected);
        fd.add_slot_with_parent(12, 11, &mut forked_detected);
        fd.add_slot_with_parent(13, 12, &mut forked_detected);

        // 3 -> 14 -> 15 -> 16 -> 17 -> 18 -> 19 -> 20
        fd.add_slot_with_parent(14, 3, &mut forked_detected);
        fd.add_slot_with_parent(15, 14, &mut forked_detected);
        fd.add_slot_with_parent(16, 15, &mut forked_detected);
        fd.add_slot_with_parent(17, 16, &mut forked_detected);
        fd.add_slot_with_parent(18, 17, &mut forked_detected);
        fd.add_slot_with_parent(19, 18, &mut forked_detected);
        fd.add_slot_with_parent(20, 19, &mut forked_detected);

        assert!(forked_detected.is_empty());

        // Forking the tree root
        fd.mark_slot_as_forked(1, &mut forked_detected);
        let expected_forks_detected = FxHashSet::from_iter([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ]);
        assert_eq!(forked_detected, expected_forks_detected);

        // It should be idempotent
        forked_detected.clear();
        fd.mark_slot_as_forked(1, &mut forked_detected);
        assert!(forked_detected.is_empty());
    }

    #[test]
    fn test_forking_subree() {
        // test case
        //
        //          14 -> 15 -> 16 -> 17 -> 18 -> 19 -> 20
        //           ^
        //           |
        // 1 -> 2 -> 3 (mark as fork) -> 4  -> 5 -> 6 -> 7 -> 8 -> 9 -> 10
        // |
        // v
        // 11 -> 12 -> 13
        //
        // Forking 3 should mark all its descendants as forks
        //

        let mut fd = Forks::with_max_capacity(1);

        let mut forked_detected = FxHashSet::default();
        fd.add_slot_with_parent(2, 1, &mut forked_detected);
        fd.add_slot_with_parent(3, 2, &mut forked_detected);
        fd.add_slot_with_parent(4, 3, &mut forked_detected);
        fd.add_slot_with_parent(5, 4, &mut forked_detected);
        fd.add_slot_with_parent(6, 5, &mut forked_detected);
        fd.add_slot_with_parent(7, 6, &mut forked_detected);
        fd.add_slot_with_parent(8, 7, &mut forked_detected);
        fd.add_slot_with_parent(9, 8, &mut forked_detected);
        fd.add_slot_with_parent(10, 9, &mut forked_detected);

        // 1 -> 11 -> 12 -> 13
        fd.add_slot_with_parent(11, 1, &mut forked_detected);
        fd.add_slot_with_parent(12, 11, &mut forked_detected);
        fd.add_slot_with_parent(13, 12, &mut forked_detected);

        // 3 -> 14 -> 15 -> 16 -> 17 -> 18 -> 19 -> 20
        fd.add_slot_with_parent(14, 3, &mut forked_detected);
        fd.add_slot_with_parent(15, 14, &mut forked_detected);
        fd.add_slot_with_parent(16, 15, &mut forked_detected);
        fd.add_slot_with_parent(17, 16, &mut forked_detected);
        fd.add_slot_with_parent(18, 17, &mut forked_detected);
        fd.add_slot_with_parent(19, 18, &mut forked_detected);
        fd.add_slot_with_parent(20, 19, &mut forked_detected);

        assert!(forked_detected.is_empty());

        // Forking the tree root
        fd.mark_slot_as_forked(3, &mut forked_detected);
        let expected_forks_detected =
            FxHashSet::from_iter([3, 4, 5, 6, 7, 8, 9, 10, 14, 15, 16, 17, 18, 19, 20]);
        assert_eq!(forked_detected, expected_forks_detected);

        // It should be idempotent
        forked_detected.clear();
        fd.mark_slot_as_forked(3, &mut forked_detected);
        assert!(forked_detected.is_empty());
    }
}
