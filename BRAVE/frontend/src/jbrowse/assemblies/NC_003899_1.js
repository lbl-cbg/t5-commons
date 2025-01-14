


export const assembly = {
    name: 'NC_003899.1',
    aliases: ['NC_003899.1'],
    sequence: {
        type: 'ReferenceSequenceTrack',
        trackId: 'NC_003899_1-ReferenceSequenceTrack',
        adapter: { 
        "type": "IndexedFastaAdapter",
        fastaLocation: {
            uri: '/assets/NC_003899_1/nc_003899_1_sequence.fasta',
        },
        faiLocation: {
            uri: '/assets/NC_003899_1/nc_003899_1_sequence.fasta.fai',
        }
        },
    }
};

/*
const assembly = {
name: 'MK028842.1',
aliases: ['MK028842.1'],
sequence: {
    type: 'ReferenceSequenceTrack',
    trackId: 'MM-ReferenceSequenceTrack',
    adapter: { 
    "type": "IndexedFastaAdapter",
    fastaLocation: {
        uri: '/assets/sequence.fasta',
    },
    faiLocation: {
        uri: '/assets/sequence.fasta.fai',
    }
    },
}
};
*/
/*
const GOODassembly = {
name: 'GRCh38',
aliases: ['hg38'],
sequence: {
    type: 'ReferenceSequenceTrack',
    trackId: 'GRCh38-ReferenceSequenceTrack',
    adapter: {
    type: 'BgzipFastaAdapter',
    fastaLocation: {
        uri: 'https://jbrowse.org/genomes/GRCh38/fasta/hg38.prefix.fa.gz',
    },
    faiLocation: {
        uri: 'https://jbrowse.org/genomes/GRCh38/fasta/hg38.prefix.fa.gz.fai',
    },
    gziLocation: {
        uri: 'https://jbrowse.org/genomes/GRCh38/fasta/hg38.prefix.fa.gz.gzi',
    },
    },
},
refNameAliases: {
    adapter: {
    type: 'RefNameAliasAdapter',
    location: {
        uri: 'https://s3.amazonaws.com/jbrowse.org/genomes/GRCh38/hg38_aliases.txt',
    },
    },
},
};
*/
