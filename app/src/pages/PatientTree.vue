<template>
  <Page>
    <main>
      <Section id="patient-tree-viz">
        <h2>Patient Tree</h2>
        <p>The <strong>Patient Tree</strong> visualizes the connection between patients, samples, and experiments. Search for records using one or more subject- or family identifiers. To search for more than one identifier, separate values by a comma like so "firstID, secondID,...". At the top level, are patients. Click a patient ID to view all linked samples. Click a sample ID to view all linked experiments.</p>
        <div class="form-chart-area">
          <Form
            id="patient-tree-search"
            title="Search for patients or families"
            class="area-aside"
          >
            <FormSection>
              <Errorbox v-if="validation.hasError">
                <span><strong>Error:</strong> {{ validation.message }}</span>
              </Errorbox>
              <InputSearch
                id="search-patient-id"
                label="Subject"
                ref="formInputSubjectID"
                description="Enter one or more subject identifier"
                @search="(value) => updateSubjectID(value)"
              />
              <InputSearch
                id="search-family-id"
                ref="formInputFamilyID"
                label="Families"
                description="Enter one or more family identifier"
                @search="(value) => updateFamilyID(value)"
              />
            </FormSection>
            <SearchButton id="search-patient-tree" @click="getData" />
          </Form>
          <div class="area-main">
            <Errorbox v-if="request.hasError" style="margin:0;">
              <p style="margin:0;"><strong>Error:</strong> {{ request.message }}</p>
            </Errorbox>
            <MessageBox v-else-if="!request.hasError && !request.isLoading && !treedata.length">
              <p style="margin:0;">Search for subjects or families to display the patient tree.</p>
            </MessageBox>
            <MessageBox v-else-if="!request.hasError && request.isLoading && !treedata.length">
              <p>Retrieving data....</p>
            </MessageBox>
            <TreeView id="patient-tree" :data="treedata" v-else/>
          </div>
        </div>
      </Section>
    </main>
  </Page>
</template>

<script>
import Page from '../components/Page.vue'
import Section from '../components/Section'
import TreeView from '../components/TreeView.vue'
import MessageBox from '../components/MessageBox.vue'
import Errorbox from '../components/Errorbox.vue'
import InputSearch from '../components/InputSearch.vue'
import SearchButton from '../components/ButtonSearch.vue'
import Form from '../components/Form.vue'
import FormSection from '../components/FormSection.vue'

import {
  fetchData,
  removeNullObjectKeys,
  objectToUrlFilterArray,
  buildFilterUrl
} from '../utils/search'

export default {
  name: 'page-patient-tree',
  components: {
    Page,
    Section,
    Form,
    FormSection,
    MessageBox,
    Errorbox,
    TreeView,
    InputSearch,
    SearchButton
  },
  data () {
    return {
      endpoint: '/api/v2/rd3stats_treedata',
      filters: {
        subjectID: null,
        familyID: null
      },
      validation: {
        hasError: false,
        message: null
      },
      request: {
        isLoading: false,
        hasError: false,
        message: null
      },
      treedata: []
    }
  },
  methods: {
    resetError (value) {
      if (value.length) {
        this.validation.hasError = false
        this.request.hasError = false
        this.request.message = null
      }
    },
    updateSubjectID (value) {
      this.filters.subjectID = value
      this.resetError(value)
    },
    updateFamilyID (value) {
      this.filters.familyID = value
      this.resetError(value)
    },
    getData () {
      const userinput = removeNullObjectKeys(this.filters)
      if (Object.keys(userinput).length === 0) {
        this.validation.hasError = true
        this.validation.message = 'All fields are blank. Enter one or more identifier to view the patient tree.'
      } else {
        this.request.isLoading = true
        const filters = objectToUrlFilterArray(userinput)
        const filterurl = buildFilterUrl(filters, ',')
        const url = this.endpoint + '?q=' + filterurl

        Promise.all([
          fetchData(url)
        ]).then(response => {
          const treedata = response[0].items
          if (treedata.length === 0) {
            const msg = `No results returned with the search parameters ${filters}`
            throw new Error(msg)
          }
          this.treedata = treedata
          this.request.isLoading = false
        }).catch(error => {
          this.request.isLoading = false
          this.request.hasError = true
          this.request.message = error.message
          this.$refs.formInputSubjectID.value = ''
          this.$refs.formInputFamilyID.value = ''
        })
      }
    }
  }
}

</script>

<style lang="scss">
.form-chart-area {
  display: grid;
  grid-template-columns: 100%;
  justify-content: flex-start;
  align-items: flex-start;
  gap: 12px;
  
  .area-main {
    background-color: #ffffff;
    box-sizing: border-box;
    padding: 2em;
    border-radius: 6px;
  }
  
  @media screen and (min-width: 762px) {
    grid-template-columns: 40% 60%;
    gap: 30px;
  }
  
}

// this is for demo purposes only
#patient-tree-viz {
  min-height: calc(100vh - 9em);
}
</style>
