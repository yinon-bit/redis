"""Selection metadata from shared spec v2.0; not an execution/compliance claim."""
import itertools

DATASETS = {
    'small_only': (11400000, 0, 0),
    'big_only': (0, 3276, 0),
    'mixed': (9600000, 410, 120000),
    'small_heavy': (11400000, 16, 0),
    'big_heavy': (128, 3276, 0),
}


def performance_cases():
    cases = {}
    for tier, datasets, lifecycles, caps in [
        ('standard', DATASETS, ('first', 'subsequent', 'after_failure'), (12, 5)),
        ('large', ('mixed', 'small_heavy', 'big_heavy'), ('first',), (40, 22)),
    ]:
        for dataset, workload, memory, cpu, lifecycle in itertools.product(
                datasets, ('idle', 'write100', 'read100', 'rw50'),
                ('ample', 'tight'), ('cpu6', 'cpu1'), lifecycles):
            case_id = '.'.join((tier, dataset, workload, memory, cpu, lifecycle, 'bgsave'))
            counts = [n*(5 if tier == 'large' else 1) for n in DATASETS[dataset]]
            cases[case_id] = dict(
                case_id=case_id, batch='core' if tier == 'standard' else 'large',
                dataset_id=dataset, workload=workload, small_keys=counts[0],
                big_keys=counts[1], hash_keys=counts[2], small_bytes=256,
                big_bytes=1048576, hash_fields=4, hash_field_bytes=32,
                memory_limit_bytes=caps[memory == 'tight']*1024**3,
                cpu_profile=cpu, lifecycle=lifecycle, scenario='bgsave',
                clients=0 if workload == 'idle' else 16,
                pipeline=1 if dataset in ('big_only', 'big_heavy') else 16,
                load_model='closed_loop_unthrottled', repetitions=3,
                before_seconds=15, after_seconds=5, save_timeout_s=600,
                validation_timeout_s=600, payload_generator='payload_v1',
                status='executable_with_deviations',
                reason='document-cases/1 driver; review recorded deviations from full shared v2.0 compliance.')
    return cases


FIXTURE_FAMILIES = [
    'arena_transition', 'hot_key_contention', 'incremental_mutation', 'empty_db',
    'multi_db', 'disk_full', 'slow_disk', 'replica_disconnect_retry', 'save_overlap',
    'normal_sync', 'slow_sync_converge', 'slow_sync_pressure', 'cancellation_retry',
    'snapshot_budget', 'point_in_time', 'metadata', 'eviction', 'defrag',
    'runtime_switch', 'aof_rewrite', 'profile',
]


def fixture_cases():
    """Explicit gated subset, not the full shared-contract fixture matrix."""
    cases = {}
    variants = ('string_grow', 'string_shrink', 'string_boundary', 'string_inplace',
                'string_setrange', 'hash_encoding', 'hash_fields', 'type_replace')
    for variant, position, memory, cpu in itertools.product(
            variants, ('before_visit', 'during_capture', 'after_visit'),
            ('ample', 'tight'), ('cpu6', 'cpu1')):
        name = '.'.join(('fixture', 'arena', variant, position, memory, cpu))
        cases[name] = dict(case_id=name, scenario='arena_transition', variant=variant,
            serializer_position=position, small_keys=9000000, target_keys=1024,
            memory_limit_bytes=(12 if memory=='ample' else 5)*1024**3,
            cpu_profile=cpu, repetitions=3, status='experimental_fixture', fixture=True)
    name = 'fixture.snapshot.base.ample.cpu6'
    cases[name] = dict(case_id=name, scenario='point_in_time', serializer_position='before_visit',
        small_keys=100000, big_keys=32, hash_keys=1000, memory_limit_bytes=12*1024**3,
        cpu_profile='cpu6', repetitions=3, status='experimental_fixture', fixture=True)
    for workload, memory, cpu in itertools.product(('idle', 'write100', 'rw50'), ('ample', 'tight'), ('cpu6', 'cpu1')):
        base = performance_cases()[f'standard.mixed.{workload}.{memory}.{cpu}.first.bgsave'].copy()
        name = '.'.join(('fixture', 'cancellation', workload, memory, cpu))
        base.update(case_id=name, scenario='cancellation_retry', fixture=True, status='experimental_fixture')
        cases[name] = base
    return cases


def runnable_cases():
    return {**performance_cases(), **fixture_cases()}


def listing():
    return dict(source='SHARED-POC-BENCHMARK-SPEC.md', spec_version='2.0',
                runnable=['custom','section6_performance','gated_fixture_subset'], performance_cases=list(performance_cases().values()),
                fixture_cases=list(fixture_cases().values()),
                fixture_families=[dict(family=f, status='partial_experimental' if f in ('arena_transition','point_in_time','cancellation_retry') else 'unsupported') for f in FIXTURE_FAMILIES],
                note='Fixture subset requires a separate instrumented fork/hybrid build; not full v2.0 compliance. Slow/disconnected replicas remain unsupported.')
