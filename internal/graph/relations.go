package graph

func StandardRelationTypes() []RelationType {
	return []RelationType{
		{
			Name:            "contains",
			DisplayName:     "contains",
			ReverseName:     "contained_by",
			Directed:        true,
			Cardinality:     ManyToMany,
			ImpactDirection: "reverse",
			AllowCrossKind:  true,
			Standard:        true,
		},
		{
			Name:            "runs_on",
			DisplayName:     "runs on",
			ReverseName:     "runs",
			Directed:        true,
			Cardinality:     ManyToMany,
			ImpactDirection: "forward",
			AllowCrossKind:  true,
			Standard:        true,
		},
		{
			Name:            "depends_on",
			DisplayName:     "depends on",
			ReverseName:     "dependency_of",
			Directed:        true,
			Cardinality:     ManyToMany,
			ImpactDirection: "forward",
			AllowCrossKind:  true,
			Standard:        true,
		},
		{
			Name:            "owned_by",
			DisplayName:     "owned by",
			ReverseName:     "owns",
			Directed:        true,
			Cardinality:     ManyToMany,
			ImpactDirection: "none",
			AllowCrossKind:  true,
			Standard:        true,
		},
		{
			Name:            "connects_to",
			DisplayName:     "connects to",
			ReverseName:     "connected_from",
			Directed:        true,
			Cardinality:     ManyToMany,
			ImpactDirection: "both",
			AllowCrossKind:  true,
			Standard:        true,
		},
	}
}
