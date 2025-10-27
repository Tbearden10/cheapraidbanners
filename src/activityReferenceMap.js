// This map contains all known referenceIds for each dungeon, keyed by the dungeon's normal mode referenceId as hash.
// Each dungeon entry includes its displayName and an array of all known activity referenceIds for variants/modes.

export const ACTIVITY_REFERENCE_MAP = [
  {
    hash: "3834447244", // Sundered Doctrine - Normal
    displayName: "Sundered Doctrine",
    referenceIds: [
      "247869137",    // Sundered Doctrine (legacy/unknown)
      "3834447244",   // Normal
      "3521648250",   // Master
    ],
  },
  {
    hash: "300092127", // Vesper's Host - Normal
    displayName: "Vesper's Host",
    referenceIds: [
      "1915770060",   // Vesper's Host (legacy/unknown)
      "300092127",    // Normal
      "4293676253",   // Master
    ],
  },
  {
    hash: "2004855007", // Warlord's Ruin - Standard
    displayName: "Warlord's Ruin",
    referenceIds: [
      "2004855007", // Standard (Normal)
      "2534833093", // Master
    ],
  },
  {
    hash: "313828469", // Ghosts of the Deep - Standard
    displayName: "Ghosts of the Deep",
    referenceIds: [
      "313828469",    // Standard (Normal)
      "124340010",    // Ultimatum
      "4190119662",   // Explorer (Matchmade)
      "1094262727",   // Explorer
      "2961030534",   // Eternity
      "2716998124",   // Master
    ],
  },
  {
    hash: "1262462921", // Spire of the Watcher - Standard
    displayName: "Spire of the Watcher",
    referenceIds: [
      "1262462921",  // Standard (Normal)
      "3339002067",  // Ultimatum
      "1225969316",  // Explorer
      "943878085",   // Explorer (Matchmade)
      "4046934917",  // Eternity
      "2296818662",  // Master
    ],
  },
  {
    hash: "2823159265", // Duality - Standard
    displayName: "Duality",
    referenceIds: [
      "2823159265", // Standard (Normal)
      "3012587626", // Master
      "1668217731", // Master (alternate)
    ],
  },
  {
    hash: "4078656646", // Grasp of Avarice - Standard (matches legacy, but this is used in metrics)
    displayName: "Grasp of Avarice",
    referenceIds: [
      "4078656646", // Standard (Normal)
      "1112917203", // Master,
      "3774021532", // Master (alternate)
    ],
  },
  {
    hash: "1077850348", // Prophecy - Prophecy
    displayName: "Prophecy",
    referenceIds: [
      "715153594",    // Eternity
      "3637651331",   // Explorer
      "1077850348",   // Prophecy (Normal)
      "3193125350",   // Ultimatum
      "1788465402",   // Explorer (Matchmade)
      "4148187374",   // Extra
    ],
  },
  {
    hash: "2582501063", // Pit of Heresy - Standard
    displayName: "Pit of Heresy",
    referenceIds: [
      "2582501063", // Standard (Normal)
      "1375089621", // Normal/legacy/alt
    ],
  },
  {
    hash: "2032534090", // The Shattered Throne - Only one known referenceId
    displayName: "The Shattered Throne",
    referenceIds: [
      "2032534090", // Only one
    ],
  },
];