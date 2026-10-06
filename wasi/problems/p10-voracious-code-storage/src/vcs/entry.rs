use std::cmp;

use super::{Error, Path, RevisionId, data_is_valid};

pub type Data = Vec<u8>;

#[derive(Debug, PartialEq, Eq)]
pub enum ListEntry {
    Dir(String, usize),
    File(String, usize),
}

impl ListEntry {
    #[must_use]
    pub fn name(&self) -> &String {
        match self {
            ListEntry::Dir(name, ..) | ListEntry::File(name, ..) => name,
        }
    }
}

impl PartialOrd for ListEntry {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ListEntry {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.name().cmp(other.name())
    }
}

pub struct List<I> {
    pub(crate) len: usize,
    pub(crate) iter: I,
}

impl<I> List<I> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<I: Iterator<Item = ListEntry>> Iterator for List<I> {
    type Item = ListEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) name: String,
    pub(crate) subdirs: Vec<Entry>,
    pub(crate) revisions: Vec<Data>,
}

impl Entry {
    pub(crate) fn name(&self) -> &String {
        &self.name
    }

    pub(crate) fn insert_revision(&mut self, data: Data) -> Result<RevisionId, Error> {
        if !data_is_valid(&data) {
            return Err(Error::InvalidData);
        }

        match self.revisions.last() {
            Some(last) => {
                if *last != data {
                    self.revisions.push(data);
                }
            }
            None => {
                self.revisions.push(data);
            }
        }

        Ok(self.revisions.len() - 1)
    }

    pub(crate) fn get_current_revision(&self) -> Result<&Data, Error> {
        self.revisions.last().ok_or(Error::RevisionNotFound)
    }

    pub(crate) fn get_revision(&self, revision_id: RevisionId) -> Result<&Data, Error> {
        self.revisions
            .get(revision_id)
            .ok_or(Error::RevisionNotFound)
    }

    pub(crate) fn dir<'a>(mut entries: &'a [Self], path: &Path) -> Result<&'a [Self], Error> {
        for component in path.components().filter(|component| !component.is_empty()) {
            if let Some(Entry { subdirs, .. }) = entries
                .iter()
                .find(|entry| entry.name().as_bytes() == component)
            {
                entries = subdirs;
            } else {
                return Err(Error::DirNotFound);
            }
        }

        Ok(entries)
    }

    pub(crate) fn file<'a>(mut entries: &'a [Self], path: &Path) -> Result<&'a Self, Error> {
        let mut candidate = None;
        for component in path.components().filter(|component| !component.is_empty()) {
            if let Some(entry) = entries
                .iter()
                .find(|entry| entry.name().as_bytes() == component)
            {
                candidate = Some(entry);

                entries = &entry.subdirs;
            } else {
                return Err(Error::DirNotFound);
            }
        }

        candidate.ok_or(Error::FileNotFound)
    }

    pub(crate) fn file_mut<'a>(mut entries: &'a mut Vec<Self>, path: &Path) -> &'a mut Self {
        let mut components = path
            .components()
            .filter(|component| !component.is_empty())
            .peekable();

        while let Some(component) = components.next() {
            if entries
                .iter()
                .position(|entry| entry.name().as_bytes() == component)
                .is_none()
            {
                let entry = Entry {
                    name: String::from_utf8_lossy(component).into_owned(),
                    subdirs: vec![],
                    revisions: vec![],
                };
                entries.push(entry);
            }

            let entry = entries
                .iter_mut()
                .find(|entry| entry.name().as_bytes() == component)
                .unwrap();

            if components.peek().is_none() {
                return entry;
            }

            entries = &mut entry.subdirs;
        }

        unreachable!("file not found?")
    }
}
