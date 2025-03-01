// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Suppliers;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The slot range required for expression analyze.
 *
 * slots: The symbols used at this level are stored in slots.
 * outerScope: The scope information corresponding to the parent is stored in outerScope.
 * ownerSubquery: The subquery corresponding to ownerSubquery.
 * subqueryToOuterCorrelatedSlots: The slots correlated in the subquery,
 *                                 only the slots that cannot be resolved at this level.
 *
 * eg:
 * t1(k1, v1) / t2(k2, v2)
 * select * from t1 where t1.k1 = (select sum(k2) from t2 where t1.v1 = t2.v2);
 *
 * When analyzing subquery:
 *
 * slots: k2, v2;
 * outerScope:
 *      slots: k1, v1;
 *      outerScope: Optional.empty();
 *      ownerSubquery: Optional.empty();
 *      subqueryToOuterCorrelatedSlots: empty();
 * ownerSubquery: subquery((select sum(k2) from t2 where t1.v1 = t2.v2));
 * subqueryToOuterCorrelatedSlots: (subquery, v1);
 */
public class Scope {

    private final Optional<Scope> outerScope;
    private final List<Slot> slots;
    private final List<Slot> asteriskSlots;
    private final Set<Slot> correlatedSlots;
    private final boolean buildNameToSlot;
    private final Supplier<ListMultimap<String, Slot>> nameToSlot;
    private final Supplier<ListMultimap<String, Slot>> nameToAsteriskSlot;

    public Scope(List<Slot> slots) {
        this(Optional.empty(), slots);
    }

    public Scope(Optional<Scope> outerScope, List<Slot> slots) {
        this(outerScope, slots, slots);
    }

    public Scope(List<Slot> slots, List<Slot> asteriskSlots) {
        this(Optional.empty(), slots, asteriskSlots);
    }

    /** Scope */
    public Scope(Optional<Scope> outerScope, List<Slot> slots, List<Slot> asteriskSlots) {
        this.outerScope = Objects.requireNonNull(outerScope, "outerScope can not be null");
        this.slots = Utils.fastToImmutableList(Objects.requireNonNull(slots, "slots can not be null"));
        this.correlatedSlots = Sets.newLinkedHashSet();
        this.buildNameToSlot = slots.size() > 500;
        this.nameToSlot = buildNameToSlot ? Suppliers.memoize(this::buildNameToSlot) : null;
        this.nameToAsteriskSlot = buildNameToSlot ? Suppliers.memoize(this::buildNameToAsteriskSlot) : null;
        this.asteriskSlots = Utils.fastToImmutableList(
                Objects.requireNonNull(asteriskSlots, "asteriskSlots can not be null"));
    }

    public List<Slot> getSlots() {
        return slots;
    }

    public List<Slot> getAsteriskSlots() {
        return asteriskSlots;
    }

    public Optional<Scope> getOuterScope() {
        return outerScope;
    }

    public Set<Slot> getCorrelatedSlots() {
        return correlatedSlots;
    }

    /** findSlotIgnoreCase */
    public List<Slot> findSlotIgnoreCase(String slotName, boolean all) {
        List<Slot> slots = all ? this.slots : this.asteriskSlots;
        Supplier<ListMultimap<String, Slot>> nameToSlot = all ? this.nameToSlot : this.nameToAsteriskSlot;
        if (!buildNameToSlot) {
            Slot[] array = new Slot[slots.size()];
            int filterIndex = 0;
            for (Slot slot : slots) {
                if (slot.getName().equalsIgnoreCase(slotName)) {
                    array[filterIndex++] = slot;
                }
            }
            return Arrays.asList(array).subList(0, filterIndex);
        } else {
            return nameToSlot.get().get(slotName.toUpperCase(Locale.ROOT));
        }
    }

    private ListMultimap<String, Slot> buildNameToSlot() {
        ListMultimap<String, Slot> map = LinkedListMultimap.create(slots.size());
        for (Slot slot : slots) {
            map.put(slot.getName().toUpperCase(Locale.ROOT), slot);
        }
        return map;
    }

    private ListMultimap<String, Slot> buildNameToAsteriskSlot() {
        ListMultimap<String, Slot> map = LinkedListMultimap.create(asteriskSlots.size());
        for (Slot slot : asteriskSlots) {
            map.put(slot.getName().toUpperCase(Locale.ROOT), slot);
        }
        return map;
    }
}
