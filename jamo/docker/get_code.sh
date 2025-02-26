git clone https://$USER:$PAT@code.jgi.doe.gov/advanced-analysis/jgi-sdm-common.git sdm-common.git
cd sdm-common.git
git checkout enh/entry_points
cd ..

git clone https://$USER:$PAT@code.jgi.doe.gov/advanced-analysis/jgi-lapinpy.git lapinpy.git
cd lapinpy.git
git checkout enh/rm_sdm_common
cd ..

git clone https://$USER:$PAT@code.jgi.doe.gov/advanced-analysis/jgi-jamo.git jamo.git
cd jamo.git
git checkout rm_sdm_common
cd ..

git clone https://$USER:$PAT@code.jgi.doe.gov/advanced-analysis/jgi-jat-ui.git jat.git
cd jat.git
git checkout package
cd ..
