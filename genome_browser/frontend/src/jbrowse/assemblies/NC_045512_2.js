
/*
Source: https://www.ncbi.nlm.nih.gov/nuccore/NC_045512.2?report=genbank
*/

export const assembly = {
    name: 'NC_045512.2',
    aliases: ['NC_045512.2'],
    sequence: {
        type: 'ReferenceSequenceTrack',
        trackId: 'NC_045512_2-ReferenceSequenceTrack',
        adapter: { 
        "type": "IndexedFastaAdapter",
        fastaLocation: {
            uri: '/assets/NC_045512_2/nc_045512_2_sequence.fasta',
        },
        faiLocation: {
            uri: '/assets/NC_045512_2/nc_045512_2_sequence.fasta.fai',
        }
        },
    }
};

