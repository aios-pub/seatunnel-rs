/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// The kind of change represented by a row in a CDC stream.
///
/// Mirrors Debezium's `Op` and SeaTunnel's `RowKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum RowKind {
    #[default]
    Insert,
    UpdateBefore,
    UpdateAfter,
    Delete,
}

impl RowKind {
    /// Returns true if this is an insert or update-after operation.
    pub const fn is_insert_like(&self) -> bool {
        matches!(self, RowKind::Insert | RowKind::UpdateAfter)
    }

    /// Returns true if this is a delete or update-before operation.
    pub const fn is_delete_like(&self) -> bool {
        matches!(self, RowKind::Delete | RowKind::UpdateBefore)
    }

    /// Decode from a single character (Debezium style).
    pub fn from_char(c: char) -> Option<Self> {
        match c {
            'c' | 'I' => Some(RowKind::Insert),
            'u' | 'U' => Some(RowKind::UpdateAfter),
            'b' => Some(RowKind::UpdateBefore),
            'd' | 'D' => Some(RowKind::Delete),
            _ => None,
        }
    }

    /// Encode to a single character.
    pub fn to_char(&self) -> char {
        match self {
            RowKind::Insert => 'c',
            RowKind::UpdateAfter => 'u',
            RowKind::UpdateBefore => 'b',
            RowKind::Delete => 'd',
        }
    }
}

impl std::fmt::Display for RowKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowKind::Insert => write!(f, "INSERT"),
            RowKind::UpdateBefore => write!(f, "UPDATE_BEFORE"),
            RowKind::UpdateAfter => write!(f, "UPDATE_AFTER"),
            RowKind::Delete => write!(f, "DELETE"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_char() {
        assert_eq!(RowKind::from_char('c'), Some(RowKind::Insert));
        assert_eq!(RowKind::from_char('I'), Some(RowKind::Insert));
        assert_eq!(RowKind::from_char('u'), Some(RowKind::UpdateAfter));
        assert_eq!(RowKind::from_char('U'), Some(RowKind::UpdateAfter));
        assert_eq!(RowKind::from_char('b'), Some(RowKind::UpdateBefore));
        assert_eq!(RowKind::from_char('d'), Some(RowKind::Delete));
        assert_eq!(RowKind::from_char('D'), Some(RowKind::Delete));
        assert_eq!(RowKind::from_char('x'), None);
    }

    #[test]
    fn test_to_char() {
        assert_eq!(RowKind::Insert.to_char(), 'c');
        assert_eq!(RowKind::UpdateAfter.to_char(), 'u');
        assert_eq!(RowKind::UpdateBefore.to_char(), 'b');
        assert_eq!(RowKind::Delete.to_char(), 'd');
    }

    #[test]
    fn test_is_insert_like() {
        assert!(RowKind::Insert.is_insert_like());
        assert!(RowKind::UpdateAfter.is_insert_like());
        assert!(!RowKind::Delete.is_insert_like());
        assert!(!RowKind::UpdateBefore.is_insert_like());
    }

    #[test]
    fn test_is_delete_like() {
        assert!(RowKind::Delete.is_delete_like());
        assert!(RowKind::UpdateBefore.is_delete_like());
        assert!(!RowKind::Insert.is_delete_like());
        assert!(!RowKind::UpdateAfter.is_delete_like());
    }
}
