<template>
  <Page>
    <Header
      id="rd3-home-header"
      title="SolveRD"
      subtitle="The rare disease database for solving the unsolved rare diseases"
    />
    <main class="container-fluid">
      <Section aria-labelledby="solverd-getstarted-title" id="solverd-getstarted" class="section-bg-plain">
        <div class="row">
          <div class="col-sm-8">
            <h2>Welcome to RD3</h2>
            <p>The RD3 &mdash;or Rare Disease Data about Data&mdash; database provides an overview of Solve-RD
              &quot;asset collections&quot;. This includes metadata and files associated with patients and samples, as
              well as an overview on the analyses that were performed.</p>
            <p>There are several ways to get started. If you are familiar with the structure of RD3, you can view the
              tables using the <a href="/menu/main/navigator">Table Navigator</a>. The <strong>Discovery Nexus
                tool</strong> can also be used to discover data within RD3. In addition, visit the &quot;Get
              Started&quot; page to find view data by release.</p>
            <ActionLink href="/#/get-started" style="margin-right: 8px;">Get Started</ActionLink>
            <ActionLink href="https://rdnexus.molgeniscloud.org/discover/index" :showExternalLinkIcon="true">
              Discovery Nexus
            </ActionLink>
          </div>
          <div id="solve-rd-logo-lg" class="col-sm-4 p-2">
            <img alt="Solve RD, Solving the Unsolved Rare Diseases" src="/logo/solve-rd-logo-icon.png" />
          </div>
        </div>
      </Section>
      <Section aria-labelledby="solverd-learn-more-title" class="mb-5" id="solverd-learn-more">
        <h2 class="h2" id="solverd-learn-more-title">Learn More</h2>
        <p>If you would like to learn more about how to use Solve-RD platform, then visit the following help pages. More
          information about the Solve-RD project can be found on on our homepage: <a
             href="http://solve-rd.eu">solve-rd.eu</a>.</p>
        <div class="row">
          <div class="col-md-4 mb-4">
            <div class="card bg-white rounded">
              <div class="card-body">
                <p class="card-title">RD3 and Sandbox User Guide</p>
                <p>The Solve-RD User Guide provides information on getting accessing and using the platform.
                </p>
                <ActionLink
                  class="btn btn-block"
                  href="https://docs.google.com/document/d/10N6kTdbyUgjzsEO3BjwV6nSKZymIyv36-jxooqDPMOc/edit#"
                  :showExternalLinkIcon="true">
                  Read
                </ActionLink>
              </div>
            </div>
          </div>
          <div class="col-md-4 mb-4">
            <div class="card bg-white rounded">
              <div class="card-body">
                <p class="card-title">Sandbox Technical Manual</p>
                <p>The Sandbox manual provides more information about accessing and using the HPC cluster.
                </p>
                <ActionLink
                  class="btn btn-block"
                  href="http://docs.gcc.rug.nl/fender/index.html"
                  :showExternalLinkIcon="true">
                  Read
                </ActionLink>
              </div>
            </div>
          </div>
          <div class="col-md-4 mb-4">
            <div class="card bg-white rounded">
              <div class="card-body">
                <p class="card-title">Solve-RD Data Model</p>
                <p>View the structure and attributes of the RD3 database. (An account is required to access
                  this page.)</p>
                <ActionLink
                  class="btn btn-block"
                  href="https://solve-rd-acc.gcc.rug.nl/menu/main/dataexplorer?entity=sys_md_Attribute&amp;mod=entitiesreport"
                  :showExternalLinkIcon="true">
                  Read
                </ActionLink>
              </div>
            </div>
          </div>
        </div>
        <p>If you would like to learn more about Molgenis, visit the <a
             href="https://molgenis.gitbook.io/molgenis/">Molgenis Documentation</a>.</p>
      </Section>
    </main>
  </Page>
</template>

<script>
import Page from '../components/Page.vue'
import Header from '../components/Header.vue'
import Section from '../components/Section.vue'
import ActionLink from '../components/ActionLink.vue'

export default {
  name: 'HomePage',
  components: {
    Page,
    Header,
    Section,
    ActionLink
  }
}
</script>

<style lang="scss">
#solve-rd-logo-lg {
  display: flex;
  align-items: center;

  img {
    display: block;
    border-radius: 50%;
    margin: 0 auto;
    width: 175px;
    box-shadow: -2px 4px 6px 2px hsla(0, 0%, 0%, 0.18);
  }
}
</style>
